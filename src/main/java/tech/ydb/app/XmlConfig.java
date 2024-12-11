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

    @XmlElement(name = "cdc")
    private final List<Cdc> cdcs = new ArrayList<>();

    public List<Cdc> getCdcs() {
        return this.cdcs;
    }

    public static class Cdc {
        @XmlAttribute(name = "changefeed", required = true)
        private String changefeed;
        @XmlAttribute(name = "consumer", required = true)
        private String consumer;

        @XmlValue
        private String query;

        public String getChangefeed() {
            return this.changefeed;
        }

        public String getConsumer() {
            return this.consumer;
        }

        public String getQuery() {
            return this.query;
        }
    }
}
