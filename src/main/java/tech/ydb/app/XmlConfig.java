package tech.ydb.app;

import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlValue;

/**
 *
 * @author Aleksandr Gorshenin
 */

@XmlRootElement
public class XmlConfig {
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final int DEFAULT_THREADS_COUNT = 1;

    @XmlElement(name = "cdc")
    private final List<Cdc> cdcs = new ArrayList<>();

    public List<Cdc> getCdcs() {
        return this.cdcs;
    }

    public static class Cdc implements CdcConfig {
        @XmlAttribute(name = "changefeed", required = true)
        private String changefeed;
        @XmlAttribute(name = "consumer", required = true)
        private String consumer;
        @XmlAttribute(name = "batchSize")
        private Integer batchSize;
        @XmlAttribute(name = "threadsCount")
        private Integer threadsCount;
        @XmlAttribute(name = "timeoutSeconds")
        private Integer timeoutSeconds;
        @XmlAttribute(name = "errorThreshold")
        private Integer errorThreshold;

        @XmlValue
        private String query;

        @Override
        public String getChangefeed() {
            return this.changefeed;
        }

        @Override
        public String getConsumer() {
            return this.consumer;
        }

        @Override
        public String getQuery() {
            return this.query;
        }

        @Override
        public int getBatchSize() {
            if (batchSize == null) {
                return DEFAULT_BATCH_SIZE;
            }
            return batchSize;
        }

        @Override
        public int getThreadsCount() {
            if (threadsCount == null) {
                return DEFAULT_THREADS_COUNT;
            }
            return threadsCount;
        }

        @Override
        public int getTimeoutSeconds() {
            if (timeoutSeconds == null) {
                return 0;
            }
            return timeoutSeconds;
        }

        @Override
        public int getErrorThreshold() {
            if (errorThreshold == null) {
                return 0;
            }
            return errorThreshold;
        }
    }
}
