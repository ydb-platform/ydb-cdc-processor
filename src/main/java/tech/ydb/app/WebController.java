package tech.ydb.app;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import tech.ydb.core.Status;

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
        return new Config(app);
    }

    @RequestMapping(path = "/status")
    public List<ReaderStatus> status() {
        return app.getReaders().stream().map(ReaderStatus::new).collect(Collectors.toList());
    }

    public static class Config {
        public final List<String> warnings;
        public final List<ReaderInfo> readers;

        public Config(Application app) {
            this.warnings = app.getWarnings();
            this.readers = app.getReaders().stream().map(ReaderInfo::new).collect(Collectors.toList());
        }
    }

    public static class ReaderInfo {
        public final String id;
        public final String changefeed;
        public final String consumer;

        public ReaderInfo(CdcReader reader) {
            this.id = reader.getId();
            this.changefeed = reader.getChangefeed();
            this.consumer = reader.getConsumer();
        }
    }

    public static class ReaderStatus {
        public final String id;
        public final boolean ok;
        public final String status;
        public final Long updated;
        public final Long processed;

        public ReaderStatus(CdcReader reader) {
            this.id = reader.getId();
            Status last = reader.getLastStatus();
            this.ok = last.isSuccess();
            this.status = last.toString();
            this.updated = reader.getLastUpdate().toEpochMilli();
            Instant vtm = reader.getLastVirtualTimestamp();
            this.processed = vtm != null ? vtm.toEpochMilli() : null;
        }
    }
}
