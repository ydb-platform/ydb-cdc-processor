package tech.ydb.app;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author Aleksandr Gorshenin
 */
@RestController
public class WebController {
    private final Application app;

    public WebController(Application app) {
        this.app = app;
    }

    @RequestMapping(path = "/stop", method = RequestMethod.POST)
    public void stop() {
        app.stop();
    }

    @RequestMapping(path = "/config")
    public Config config() {
        return new Config();
    }

    public class Config {
        public List<String> getWarnings() {
            return app.getWarnings();
        }

        public List<ReaderInfo> getReaders() {
            return app.getReaders().stream().map(ReaderInfo::new).collect(Collectors.toList());
        }
    }

    public class ReaderInfo {
        private final CdcReader reader;

        public ReaderInfo(CdcReader reader) {
            this.reader = reader;
        }

        public String getId() {
            return reader.getId();
        }

        public String getChangefeed() {
            return reader.getChangefeed();
        }

        public String getConsumer() {
            return reader.getConsumer();
        }
    }
}
