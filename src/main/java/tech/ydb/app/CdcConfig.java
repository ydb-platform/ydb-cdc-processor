package tech.ydb.app;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface CdcConfig {
    String getChangefeed();

    String getConsumer();

    String getQuery();

    int getBatchSize();

    int getThreadsCount();

    int getTimeoutSeconds();

    int getErrorThreshold();
}
