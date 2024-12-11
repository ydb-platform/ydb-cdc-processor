package tech.ydb.app;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.settings.ReadEventHandlersSettings;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YqlWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(YqlWriter.class);

    private final String queryYql;
    private final String paramName;
    private final StructType paramType;

    private final ConcurrentLinkedQueue<Message> queue = new ConcurrentLinkedQueue<>();

    private volatile Status lastStatus;
    private volatile Instant lastReaded;
    private volatile Instant lastWrited;

    private YqlWriter(String queryYql, String paramName, StructType paramType) {
        this.queryYql = queryYql;
        this.paramName = paramName;
        this.paramType = paramType;
        this.lastStatus = Status.SUCCESS;
        this.lastWrited = null;
        this.lastReaded = null;
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

    @Override
    public void close() {
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

        return Result.success(new YqlWriter(queryYql, paramName, (StructType) innerType));
    }
}
