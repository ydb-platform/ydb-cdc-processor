package tech.ydb.app;

import java.util.UUID;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class CdcReader {
    private final String id;
    private final String consumer;
    private final String changefeed;

    private final String queryYql;

    public CdcReader(String consumer, String changefeed, String queryYql) {
        this.id = UUID.randomUUID().toString();

        this.consumer = consumer;
        this.changefeed = changefeed;
        this.queryYql = queryYql;
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

}
