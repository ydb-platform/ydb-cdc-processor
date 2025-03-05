package tech.ydb.app;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.CommitOffsetAcknowledgementEvent;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class CdcReader implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CdcReader.class);

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
                .setDecompressionExecutor(Runnable::run)   // Prevent OOM
                .setMaxMemoryUsageBytes(200 * 1024 * 1024) // 200 Mb
                .addTopic(TopicReadSettings.newBuilder()
                        .setPath(ydb.expandPath(changefeed))
                        .build())
                .build();
        ReadEventHandlersSettings rehs = ReadEventHandlersSettings.newBuilder()
                .setEventHandler(new CdcEventHandler())
                .build();

        this.reader = ydb.createReader(rs, rehs);
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

    private class CdcEventHandler extends AbstractReadEventHandler {
        @Override
        public void onMessages(DataReceivedEvent event) {
            for (Message msg: event.getMessages()) {
                writer.addMessage(event.getPartitionSession().getPartitionId(), msg);
            }
        }

        @Override
        public void onCommitResponse(CommitOffsetAcknowledgementEvent event) {
            logger.trace("committed offset {} in topic {}[partition {}]",
                    event.getCommittedOffset(), changefeed, event.getPartitionSession().getPartitionId());
        }
    }
}
