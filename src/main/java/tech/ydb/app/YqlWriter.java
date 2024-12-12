package tech.ydb.app;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;
import tech.ydb.topic.read.DeferredCommitter;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.settings.ReadEventHandlersSettings;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YqlWriter implements AutoCloseable {
    private static final int WRITE_BATCH_SIZE = 50;
    private static final Logger logger = LoggerFactory.getLogger(YqlWriter.class);

    private final YdbService ydb;
    private final String queryYql;
    private final String paramName;
    private final StructType paramType;

    private final ConcurrentLinkedQueue<Message> queue = new ConcurrentLinkedQueue<>();
    private final Thread worker;

    private volatile Status lastStatus;
    private volatile Instant lastReaded;
    private volatile Instant lastWrited;

    private YqlWriter(YdbService ydb, String consumer, String queryYql, String paramName, StructType paramType) {
        this.ydb = ydb;
        this.queryYql = queryYql;
        this.paramName = paramName;
        this.paramType = paramType;
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

    public ReadEventHandlersSettings toHanlderSettings() {
        return ReadEventHandlersSettings.newBuilder()
                .setEventHandler(new CdcEventHandler())
                .build();
    }

    private class CdcEventHandler extends AbstractReadEventHandler {
        @Override
        public void onMessages(DataReceivedEvent event) {
            for (Message msg: event.getMessages()) {
                queue.add(msg);
                lastReaded = msg.getWrittenAt();
            }
        }
    }

    private class WriterRunnable implements Runnable {

        public Value<?> parseMsg(byte[] json) {
            return PrimitiveValue.newText(String.valueOf(json));
        }

        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run() {
            try {
                Random rnd = new Random();
                long lastPrinted = System.currentTimeMillis();
                long written = 0;

                while (true) {
                    long now = System.currentTimeMillis();
                    if (now - lastPrinted > 1000) {
                        long ms = now - lastPrinted;
                        double avg = 1000.0d * written / ms;
                        logger.debug("writed {} rows, {} rps", written, avg);
                        written = 0;
                        lastPrinted = now;
                    }

                    Message msg = queue.poll();
                    if (msg == null) {
                        Thread.sleep(1000);
                        continue;
                    }

                    DeferredCommitter committer = DeferredCommitter.newInstance();
                    List<Value<?>> values = new ArrayList<>();
                    Instant last = msg.getCreatedAt();

                    while (msg != null && values.size() < WRITE_BATCH_SIZE) {
                        values.add(PrimitiveValue.newText(String.valueOf(msg.getData())));
                        committer.add(msg);

                        msg = queue.poll();
                    }

                    Params prm = Params.of(paramName, ListType.of(paramType).newValue(values));
                    lastStatus = ydb.executeQuery(queryYql, prm);
                    if (Thread.interrupted()) {
                        return;
                    }

                    int retry = 0;
                    while (!lastStatus.isSuccess()) {
                        retry++;
                        long delay = 25 << Math.max(retry, 8);
                        delay = delay + rnd.nextLong(delay);
                        logger.debug("got error {}, retry #{} in {} ms", lastStatus, retry, delay);
                        Thread.sleep(delay);
                        lastStatus = ydb.executeQuery(queryYql, prm);
                        if (Thread.interrupted()) {
                            return;
                        }
                    }

                    committer.commit();
                    lastWrited = last;
                }
            } catch (InterruptedException e) {
                // stoppping
            }
        }

    }

    public static Result<YqlWriter> parse(YdbService ydb, String consumer, String queryYql) {
        Result<DataQuery> parsed = ydb.parseQuery(queryYql);
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

        Type innerType = ((ListType) listType).getItemType();
        if (innerType.getKind() != Type.Kind.STRUCT) {
            return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                    "Expected type List<Struct<...>>, but got " + listType, Issue.Severity.ERROR
            )));
        }

        return Result.success(new YqlWriter(ydb, consumer, queryYql, paramName, (StructType) innerType));
    }
}
