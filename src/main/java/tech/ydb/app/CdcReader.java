package tech.ydb.app;

import java.util.UUID;

import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class CdcReader implements AutoCloseable {
    private final String id;
    private final String consumer;
    private final String changefeed;

    private final AsyncReader reader;
    private final YqlWriter writer;

    public CdcReader(YdbService ydb, YqlWriter writer, String consumer, String changefeed) {
        this.id = UUID.randomUUID().toString();
        this.consumer = consumer;
        this.changefeed = changefeed;
        this.writer = writer;

        ReaderSettings rs = ReaderSettings.newBuilder()
                .setConsumerName(consumer)
                .setMaxMemoryUsageBytes(40 * 1024 * 1024) // 40 Mb
                .addTopic(TopicReadSettings.newBuilder().setPath(changefeed).build())
                .build();

        this.reader = ydb.createReader(rs, writer.toHanlderSettings());
    }

    public void start() {
        this.reader.init();
        this.writer.start();
    }

    @Override
    public void close() {
        writer.close();
        reader.shutdown();
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

    public YqlWriter getWriter() {
        return writer;
    }
}
