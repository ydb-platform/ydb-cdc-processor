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

    @XmlElement(name = "query")
    private final List<Query> queries = new ArrayList<>();

    public List<Cdc> getCdcs() {
        return this.cdcs;
    }

    public List<Query> getQueries() {
        return this.queries;
    }

    public static class Query {
        @XmlAttribute(name = "id", required = true)
        private String id;

        @XmlAttribute(name = "actionMode")
        private String actionMode;

        @XmlAttribute(name = "actionTable")
        private String actionTable;

//        @XmlAttribute(name = "batchSize")
//        private Integer batchSize;

        @XmlValue
        private String text;

        public Query() {
        }

        public Query(String query) {
            this.id = "inplacement";
            this.text = query;
        }

        public String getId() {
            return this.id;
        }

        public String getText() {
            return this.text;
        }

        public String getActionMode() {
            return this.actionMode;
        }

        public String getActionTable() {
            return this.actionTable;
        }

//        public int getBatchSize() {
//            if (batchSize == null) {
//                return DEFAULT_BATCH_SIZE;
//            }
//            return batchSize;
//        }
    }

    public static class Cdc {
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

        @XmlAttribute(name = "updateQueryId")
        private String updateQueryId;
        @XmlAttribute(name = "deleteQueryId")
        private String deleteQueryId;

        @XmlValue
        private String query;

        public String getChangefeed() {
            return this.changefeed;
        }

        public String getConsumer() {
            return this.consumer;
        }

        public String getUpdateQueryId() {
            return this.updateQueryId;
        }

        public String getDeleteQueryId() {
            return this.deleteQueryId;
        }

        public String getQuery() {
            return this.query;
        }

        public int getBatchSize() {
            if (batchSize == null) {
                return DEFAULT_BATCH_SIZE;
            }
            return batchSize;
        }

        public int getThreadsCount() {
            if (threadsCount == null) {
                return DEFAULT_THREADS_COUNT;
            }
            return threadsCount;
        }

        public int getTimeoutSeconds() {
            if (timeoutSeconds == null) {
                return 0;
            }
            return timeoutSeconds;
        }

        public int getErrorThreshold() {
            if (errorThreshold == null) {
                return 0;
            }
            return errorThreshold;
        }
    }
}
