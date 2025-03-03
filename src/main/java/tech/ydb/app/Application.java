package tech.ydb.app;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

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
                    XmlConfig xml = JAXB.unmarshal(config, XmlConfig.class);
                    for (XmlConfig.Cdc cdc: xml.getCdcs()) {
                        String changefeed = ydb.expandPath(cdc.getChangefeed());
                        String consumer = cdc.getConsumer();
                        String yqlQuery = cdc.getQuery();
                        Integer batchSize = cdc.getBatchSize();
                        if (batchSize == null) {
                            batchSize = YqlWriter.DEFAULT_BATCH_SIZE;
                        }
                        Result<YqlWriter> writer = YqlWriter.parse(ydb, changefeed, consumer, yqlQuery, batchSize);
                        if (!writer.isSuccess()) {
                            logger.error("can't create reader {} with problem {}",
                                    cdc.getConsumer(), writer.getStatus());
                            warnings.add("can't create reader " + cdc.getConsumer()
                                    + " with problem: " + writer.getStatus());
                        } else {
                            readers.add(new CdcReader(ydb, writer.getValue(), cdc.getConsumer(), cdc.getChangefeed()));
                        }
                    }
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