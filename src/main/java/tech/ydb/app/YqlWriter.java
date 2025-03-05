package tech.ydb.app;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
    private final String queryYql;

    private final List<Writer> writers;

    private volatile Status lastStatus;
    private volatile Instant lastReaded;
    private volatile Instant lastWrited;

    private final AtomicLong lastPrinted = new AtomicLong();
    private final AtomicLong writtenCount = new AtomicLong();

    private YqlWriter(YdbService ydb, CdcConfig config, String prmName, StructType type, TableDescription description) {
        this.ydb = ydb;
        this.queryYql = config.getQuery();
        this.lastStatus = Status.SUCCESS;
        this.lastWrited = null;
        this.lastReaded = null;
        this.writers = new ArrayList<>(config.getThreadsCount());

        for (int idx = 1; idx <= config.getThreadsCount(); idx++) {
            String name = "writer-" + config.getConsumer() + "[" + idx + "]";
            writers.add(new Writer(config.getBatchSize(), name, prmName, type, description));
        }
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
        lastPrinted.set(System.currentTimeMillis());
        writers.forEach(Writer::start);
    }

    @Override
    public void close() {
        writers.forEach(Writer::stop);

        try {
            for (Writer writer: writers) {
                writer.join();
            }
        } catch (InterruptedException ex) {
            logger.error("unexpected interrupt", ex);
        }
    }

    public void addMessage(long partitionId, Message msg) {
        int idx = (int) partitionId % writers.size();
        writers.get(idx).addMesssage(msg);
    }

    private class Writer implements Runnable {
        private final BlockingQueue<Message> queue;
        private final CdcMsgParser parser;
        private final Thread thread;

        public Writer(int batchSize, String threadName, String prmName, StructType type, TableDescription description) {
            this.queue = new ArrayBlockingQueue<>(2 * batchSize);
            this.parser = new CdcMsgParser(prmName, type, description, batchSize);
            this.thread = new Thread(this, threadName);
        }

        public void start() {
            thread.start();
            logger.info("writer {} started", thread.getName());
        }

        public void stop() {
            thread.interrupt();
            logger.info("writer {} stopped", thread.getName());
        }

        public void join() throws InterruptedException {
            thread.join();
            logger.info("writer {} finished", thread.getName());
        }

        public void addMesssage(Message msg) {
            try {
                while (!queue.offer(msg, 5, TimeUnit.SECONDS)) {
                    if (!thread.isAlive() || thread.isInterrupted()) {
                        return;
                    }
                }
                lastReaded = msg.getWrittenAt();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                logger.warn("worker thread was interrupted");
            }
        }

        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run() {
            try {
                Random rnd = new Random();

                while (!Thread.interrupted()) {
                    Message msg = queue.poll();
                    if (msg == null) {
                        Thread.sleep(1000);
                        continue;
                    }

                    long now = System.currentTimeMillis();
                    long printedAt = lastPrinted.get();
                    if ((now - printedAt > 1000) && lastPrinted.compareAndSet(printedAt, now)) {
                        long ms = now - printedAt;
                        long written = writtenCount.getAndSet(0);
                        double avg = 1000.0d * written / ms;
                        logger.debug("writed {} rows, {} rps", written, String.format("%.2f", avg));
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

                    writtenCount.addAndGet(parser.batchSize());
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

    public static Result<YqlWriter> parse(YdbService ydb, CdcConfig config) {
        String changefeed = ydb.expandPath(config.getChangefeed());

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

        Result<DataQuery> parsed = ydb.parseQuery(config.getQuery());
        if (!parsed.isSuccess()) {
            logger.error("Can't parse query for consumer {}, got status {}", config.getConsumer(), parsed.getStatus());
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

        return Result.success(new YqlWriter(ydb, config, paramName, structType, description));
    }
}
