package tech.ydb.app;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.JAXB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 *
 * @author Aleksandr Gorshenin
 */
@SpringBootApplication
public class Application implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private final ApplicationContext ctx;

    private final List<String> warnings = new ArrayList<>();
    private final List<CdcReader> readers = new ArrayList<>();

    public Application(ApplicationContext ctx) {
        this.ctx = ctx;
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
            logger.info("read config file {}", arg);
            File config = new File(arg);
            if (!config.exists() || !config.canRead()) {
                warnings.add("Can't read file " + arg);
            } else {
                try {
                    XmlConfig xml = JAXB.unmarshal(config, XmlConfig.class);
                    for (XmlConfig.Cdc cdc: xml.getCdcs()) {
                        readers.add(new CdcReader(cdc.getConsumer(), cdc.getChanefeed(), cdc.getQuery()));
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

        logger.info("app has started");
    }

    public void stop() {
        logger.info("app has stopped");
        SpringApplication.exit(ctx);
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}