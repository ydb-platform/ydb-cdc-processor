package tech.ydb.app;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.annotation.PreDestroy;
import jakarta.xml.bind.JAXB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import tech.ydb.core.Result;

/**
 *
 * @author Aleksandr Gorshenin
 */
@SpringBootApplication
public class Application implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private final ApplicationContext ctx;
    private final YdbService ydb;

    private final List<String> warnings = new ArrayList<>();
    private final List<CdcReader> readers = new ArrayList<>();

    public Application(ApplicationContext ctx, YdbService ydb) {
        this.ctx = ctx;
        this.ydb = ydb;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public List<CdcReader> getReaders() {
        return readers;
    }

    @Override
    public void run(String... args) {
        for (String arg : args) {
            if (arg.startsWith("--")) {
                continue;
            }

            logger.info("read config file {}", arg);
            File config = new File(arg);
            if (!config.exists() || !config.canRead()) {
                warnings.add("Can't read file " + arg);
            } else {
                try {
                    loadXmlConfig(config.toURI().toASCIIString());
                } catch (RuntimeException ex) {
                    logger.warn("can't parse file {}", arg, ex);
                    warnings.add("Parse exception: " + ex.getMessage());
                }
            }
        }

        if (readers.isEmpty()) {
            warnings.add("No reader configs found!!");
        }

        for (CdcReader reader: readers) {
            reader.start();
        }

        logger.info("app has started");
    }

    private void loadXmlConfig(String content) {
        XmlConfig xml = JAXB.unmarshal(content, XmlConfig.class);
        Map<String, XmlConfig.Query> queries = new HashMap();
        for (XmlConfig.Query query: xml.getQueries()) {
            queries.put(query.getId(), query);
        }

        for (XmlConfig.Cdc cdc: xml.getCdcs()) {
            Result<CdcMsgParser> batcher = CdcMsgParser.parse(ydb, queries, cdc);
            if (!batcher.isSuccess()) {
                logger.error("can't create reader {} with problem {}", cdc.getConsumer(), batcher.getStatus());
                warnings.add("can't create reader " + cdc.getConsumer() + " with problem: " + batcher.getStatus());
            } else {
                YqlWriter writer = new YqlWriter(ydb, batcher.getValue(), cdc);
                readers.add(new CdcReader(ydb, writer, cdc.getConsumer(), cdc.getChangefeed()));
            }
        }
    }

    @PreDestroy
    public void preDestroy() {
        logger.info("app has closed");
        for (CdcReader reader: readers) {
            reader.close();
        }
    }

    public void stop() {
        logger.info("app has stopped");
        SpringApplication.exit(ctx);
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}