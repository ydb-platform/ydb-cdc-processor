package tech.ydb.app;

import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.table.query.DataQuery;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class CdcReader {
    private static final Logger logger = LoggerFactory.getLogger(CdcReader.class);

    private final String id;
    private final String consumer;
    private final String changefeed;

    private final String queryYql;

    private volatile Status lastStatus;
    private volatile Instant lastUpdate;
    private volatile Instant lastVirtulaTimestamp;

    public CdcReader(YdbService ydb, String consumer, String changefeed, String queryYql) {
        this.id = UUID.randomUUID().toString();

        this.consumer = consumer;
        this.changefeed = changefeed;
        this.queryYql = queryYql;

        Result<DataQuery> parsed = ydb.parseQuery(queryYql);
        if (!parsed.isSuccess()) {
            logger.error("Can't parse query for consumer {}, got status {}", consumer, parsed.getStatus());
        }

        this.lastUpdate = Instant.now();
        this.lastStatus = parsed.getStatus();
        this.lastVirtulaTimestamp = null;
    }

    public String getId() {
        return this.id;
    }

    public String getConsumer() {
        return this.consumer;
    }

    public String getChangefeed() {
        return this.changefeed;
    }

    public Status getLastStatus() {
        return lastStatus;
    }

    public Instant getLastUpdate() {
        return lastUpdate;
    }

    public Instant getLastVirtualTimestamp() {
        return lastVirtulaTimestamp;
    }
}
