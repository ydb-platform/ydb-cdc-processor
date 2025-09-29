package tech.ydb.app;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;
import tech.ydb.query.impl.QueryClientImpl;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.query.Params;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReaderSettings;

/**
 *
 * @author Aleksandr Gorshenin
 */
@Service
public class YdbService {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private final static String PREFIX = "ydb.connection.";
    private final static String PARAM_URL = "url";
    private final static String PARAM_USERNAME = "username";
    private final static String PARAM_PASSWORD = "password";
    private final static String PARAM_SA_KEY = "saKeyFile";
    private final static String PARAM_TOKEN_FILE = "tokenFile";
    private final static String PARAM_CA_CERT = "caCertFile";

    private final GrpcTransport transport;

    private final TableClient tableClient;
    private final QueryClientImpl queryClient;
    private final TopicClient topicClient;

    public YdbService(Environment env) {
        String url = env.getProperty(PREFIX + PARAM_URL, "grpc://localhost:2136/local");
        Map<String, String> options = parseOptions(url);

        String username = env.getProperty(PREFIX + PARAM_USERNAME);
        String password = env.getProperty(PREFIX + PARAM_PASSWORD);
        String saKeyFile = env.getProperty(PREFIX + PARAM_SA_KEY, options.get(PARAM_SA_KEY.toLowerCase()));
        String tokenFile = env.getProperty(PREFIX + PARAM_TOKEN_FILE, options.get(PARAM_TOKEN_FILE.toLowerCase()));
        String caCartFile = env.getProperty(PREFIX + PARAM_CA_CERT, options.get(PARAM_CA_CERT.toLowerCase()));

        logger.info("connect to YDB with url {}", url);
        GrpcTransportBuilder builder = GrpcTransport.forConnectionString(url)
                .withInitMode(GrpcTransportBuilder.InitMode.ASYNC);

        if (caCartFile != null && !caCartFile.isEmpty()) {
            try {
                builder = builder.withSecureConnection(Files.readAllBytes(Path.of(caCartFile)));
            } catch (IOException ex) {
                logger.error("cannot read file {}", caCartFile, ex);
            }
        }
        if (saKeyFile != null && !saKeyFile.isEmpty()) {
            builder = builder.withAuthProvider(CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyFile));
        }
        if (tokenFile != null && !tokenFile.isEmpty()) {
            try {
                builder = builder.withAuthProvider(new TokenAuthProvider(Files.lines(Path.of(tokenFile)).findFirst().get()));
            } catch (IOException ex) {
                logger.error("cannot read file {}", tokenFile, ex);
            }
        }
        if (username != null && !username.isEmpty()) {
            builder = builder.withAuthProvider(new StaticCredentials(username, password));
        }

        this.transport = builder.build();
        this.tableClient = TableClient.newClient(transport).build();
        this.queryClient = QueryClientImpl.newClient(transport).build();
        this.topicClient = TopicClient.newClient(transport)
                .setCompressionExecutor(Runnable::run)
                .build();
    }

    public void updatePoolSize(int maxSize) {
        logger.error("set session pool max size {}", maxSize);
        queryClient.updatePoolMaxSize(maxSize);
    }

    @PreDestroy
    public void close() {
        this.topicClient.close();
        this.tableClient.close();
        this.transport.close();
    }

    public String expandPath(String name) {
        if (name == null || name.isEmpty() || name.startsWith("/")) {
            return name;
        }
        StringBuilder sb = new StringBuilder();
        String database = transport.getDatabase();
        if (!database.startsWith("/")) {
            sb.append("/");
        }
        sb.append(database);
        if (!database.endsWith("/")) {
            sb.append("/");
        }
        sb.append(name);
        return sb.toString();
    }

    @SuppressWarnings("null")
    public Result<DataQuery> parseQuery(String query) {
        Result<Session> session = tableClient.createSession(Duration.ofSeconds(5)).join();
        if (!session.isSuccess()) {
            return session.map(null);
        }

        try (Session s = session.getValue()) {
            return s.prepareDataQuery(query).join();
        }
    }

    @SuppressWarnings("null")
    public Result<TableDescription> describeTable(String tablePath) {
        Result<Session> session = tableClient.createSession(Duration.ofSeconds(5)).join();
        if (!session.isSuccess()) {
            return session.map(null);
        }

        try (Session s = session.getValue()) {
            return s.describeTable(tablePath).join();
        }
    }

    public Status executeYqlQuery(String query, Params params, int timeoutSeconds) {
        Result<QuerySession> session = queryClient.createSession(Duration.ofSeconds(5)).join();
        if (!session.isSuccess()) {
            return session.getStatus();
        }

        try (QuerySession s = session.getValue()) {
            ExecuteQuerySettings.Builder settings = ExecuteQuerySettings.newBuilder();
            if (timeoutSeconds > 0) {
                settings.withRequestTimeout(Duration.ofSeconds(timeoutSeconds));
            }
            return s.createQuery(query, TxMode.NONE, params, settings.build()).execute().join().getStatus();
        }
    }

    @SuppressWarnings("null")
    public Result<QueryReader> readYqlQuery(String query, Params params, int timeoutSeconds) {
        Result<QuerySession> session = queryClient.createSession(Duration.ofSeconds(5)).join();
        if (!session.isSuccess()) {
            return session.map(null);
        }

        try (QuerySession s = session.getValue()) {
            ExecuteQuerySettings.Builder settings = ExecuteQuerySettings.newBuilder();
            if (timeoutSeconds > 0) {
                settings.withRequestTimeout(Duration.ofSeconds(timeoutSeconds));
            }
            QueryStream stream = s.createQuery(query, TxMode.SNAPSHOT_RO, params, settings.build());
            return QueryReader.readFrom(stream).join();
        }
    }

    public AsyncReader createReader(ReaderSettings rs, ReadEventHandlersSettings settings) {
        return topicClient.createAsyncReader(rs, settings);
    }

    private static Map<String, String> parseOptions(String url) {
        Map<String, String> map = new HashMap<>();
        int question = url.indexOf('?');
        if (question < 0) {
            return map;
        }
        for (String option: url.substring(question + 1).split("&")) {
            int idx = option.indexOf("=");
            String key = idx > 0 ? option.substring(0, idx) : option;
            String value = idx > 0 && option.length() > idx + 1 ? option.substring(idx + 1) : null;
            map.put(decode(key).toLowerCase(), value == null ? null : decode(value));
        }

        return map;
    }

    private static String decode(String url) {
        try {
            return URLDecoder.decode(url, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException ex) {
            return url;
        }
    }
}
