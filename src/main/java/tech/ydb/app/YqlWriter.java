package tech.ydb.app;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;
import tech.ydb.topic.read.DeferredCommitter;
import tech.ydb.topic.read.Message;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YqlWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(YqlWriter.class);

    private final YdbService ydb;
    private final CdcMsgParser parser;
    private final String queryYql;

    private final ConcurrentLinkedQueue<Message> queue = new ConcurrentLinkedQueue<>();
    private final Thread worker;

    private volatile Status lastStatus;
    private volatile Instant lastReaded;
    private volatile Instant lastWrited;

    private YqlWriter(YdbService ydb, CdcMsgParser parser, String consumer, String queryYql) {
        this.ydb = ydb;
        this.queryYql = queryYql;
        this.parser = parser;
        this.lastStatus = Status.SUCCESS;
        this.lastWrited = null;
        this.lastReaded = null;
        this.worker = new Thread(new WriterRunnable(), "writer[" + consumer + "]");
    }

    public Status getLastStatus() {
        return lastStatus;
    }

    public Instant getLastWrited() {
        return lastWrited;
    }

    public Instant getLastReaded() {
        return lastReaded;
    }

    public void start() {
        this.worker.start();
    }

    @Override
    public void close() {
        try {
            worker.interrupt();
            worker.join();
        } catch (InterruptedException ex) {
            logger.error("unexpected interrupt", ex);
        }
    }

    public void addMessage(Message msg) {
        queue.add(msg);
        lastReaded = msg.getWrittenAt();
    }

    private class WriterRunnable implements Runnable {
        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run() {
            try {
                Random rnd = new Random();
                long lastPrinted = System.currentTimeMillis();
                long written = 0;

                while (!Thread.interrupted()) {
                    Message msg = queue.poll();
                    if (msg == null) {
                        Thread.sleep(1000);
                        continue;
                    }

                    long now = System.currentTimeMillis();
                    if (now - lastPrinted > 1000) {
                        long ms = now - lastPrinted;
                        double avg = 1000.0d * written / ms;
                        logger.debug("writed {} rows, {} rps", written, avg);
                        written = 0;
                        lastPrinted = now;
                    }

                    DeferredCommitter committer = DeferredCommitter.newInstance();
                    Instant last = msg.getCreatedAt();

                    while (msg != null) {
                        last = msg.getCreatedAt();
                        parser.addMessage(msg.getData());
                        committer.add(msg);

                        if (parser.isFull()) {
                            break;
                        }

                        msg = queue.poll();
                    }

                    if (parser.isEmpty()) {
                        committer.commit();
                        continue;
                    }

                    written += parser.batchSize();
                    Params prm = parser.build();
                    lastStatus = ydb.executeQuery(queryYql, prm);

                    int retry = 0;
                    while (!lastStatus.isSuccess()) {
                        retry++;
                        long delay = 25 << Math.min(retry, 8);
                        delay = delay + rnd.nextLong(delay);
                        logger.warn("got error {}", lastStatus);
                        logger.warn("retry #{} in {} ms", retry, delay);
                        Thread.sleep(delay);
                        lastStatus = ydb.executeQuery(queryYql, prm);
                    }

                    parser.clear();
                    committer.commit();
                    lastWrited = last;
                }
            } catch (IOException ex) {
                logger.error("writer has stopped by exception", ex);
                lastStatus = Status.of(StatusCode.CLIENT_INTERNAL_ERROR, ex,
                        Issue.of(ex.getMessage(), Issue.Severity.ERROR));
            } catch (InterruptedException ex) {
                // stoppping
            }
        }

    }

    public static Result<YqlWriter> parse(YdbService ydb, String changefeed,
            String consumer, String yqlQuery, Long batchSize) {
        int index = changefeed.lastIndexOf("/");
        if (index <= 0) {
            return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                    "Changefeed name have to contain table name with  / " + changefeed, Issue.Severity.ERROR
            )));
        }

        Result<TableDescription> descRes = ydb.describeTable(changefeed.substring(0, index));
        if (!descRes.isSuccess()) {
            logger.error("Can't describe table for changefeed {}, got status {}", changefeed, descRes.getStatus());
            return descRes.map(null);
        }
        TableDescription description = descRes.getValue();

        Result<DataQuery> parsed = ydb.parseQuery(yqlQuery);
        if (!parsed.isSuccess()) {
            logger.error("Can't parse query for consumer {}, got status {}", consumer, parsed.getStatus());
            return parsed.map(null);
        }

        Map<String, Type> types = parsed.getValue().types();
        if (types.size() != 1) {
            return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                    "Expected only one parameter, but got " + String.join(",", types.keySet()), Issue.Severity.ERROR
            )));
        }

        String paramName = types.keySet().iterator().next();
        Type listType = types.values().iterator().next();
        if (listType.getKind() != Type.Kind.LIST) {
            return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                    "Expected type List<Struct<...>>, but got " + listType, Issue.Severity.ERROR
            )));
        }

        Type itemType = ((ListType) listType).getItemType();
        if (itemType.getKind() != Type.Kind.STRUCT) {
            return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                    "Expected type List<Struct<...>>, but got " + listType, Issue.Severity.ERROR
            )));
        }

        descRes.getValue();
        StructType structType = (StructType) itemType;
        Map<String, Type> tableTypes = new HashMap<>();
        for (TableColumn column: description.getColumns()) {
            tableTypes.put(column.getName(), column.getType());
        }

        for (int idx = 0; idx < structType.getMembersCount(); idx += 1) {
            String name = structType.getMemberName(idx);
            Type type = structType.getMemberType(idx);
            if (!tableTypes.containsKey(name)) {
                return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                        "Source table doesn't have column " + name, Issue.Severity.ERROR
                )));
            }

            if (!type.equals(tableTypes.get(name))) {
                return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                        "Source table column " + name + " has type " + tableTypes.get(name) + " instead of " + type,
                        Issue.Severity.ERROR
                )));
            }
        }

        CdcMsgParser parser = new CdcMsgParser(paramName, structType, description, batchSize);
        return Result.success(new YqlWriter(ydb, parser, consumer, yqlQuery));
    }
}
