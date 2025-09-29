package tech.ydb.app;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class CdcMsgParser {
    private static final Logger logger = LoggerFactory.getLogger(YqlWriter.class); // use logger of YdlWriter

    private final ObjectMapper mapper = new ObjectMapper();

    private final YqlQuery updateQuery;
    private final YqlQuery deleteQuery;

    private CdcMsgParser(Supplier<YqlQuery> updateQuery, Supplier<YqlQuery> deleteQuery) {
        this.updateQuery = updateQuery.get();
        this.deleteQuery = deleteQuery.get();
    }

    public YqlQuery parseJsonMessage(byte[] json) throws IOException {
        JsonNode root = mapper.readTree(json);
        if (!root.isObject() || !root.hasNonNull("key")) {
            logger.error("unsupported cdc message {}", new String(json));
            return null;
        }

        JsonNode key = root.get("key");

        if (!key.isArray()) {
            logger.error("unsupported cdc message {}", new String(json));
            return null;
        }

        if (root.hasNonNull("update") && updateQuery != null) {
            JsonNode update = root.get("update");
            if (update.isObject() && update.isObject()) {
                updateQuery.addMessage(key, update.isEmpty() ? null : update);
                return updateQuery;
            }

            JsonNode newImage = root.get("newImage");
            if (newImage != null && newImage.isObject() && !newImage.isEmpty()) {
                updateQuery.addMessage(key, newImage);
                return updateQuery;
            }

            logger.error("unsupported update cdc message {}", new String(json));
            return null;
        }

        if (root.hasNonNull("erase") && deleteQuery != null) {
            deleteQuery.addMessage(key, null);
            return deleteQuery;
        }

        logger.error("unsupported cdc message {}", new String(json));
        return null;
    }

    public static Result<Supplier<CdcMsgParser>> parseConfig(YdbService ydb,
            Map<String, XmlConfig.Query> queries, XmlConfig.Cdc cdc) {
        return new Parser(ydb, cdc, queries).parse();
    }

    private static class Parser {
        private final YdbService ydb;
        private final XmlConfig.Cdc cdc;
        private final Map<String, XmlConfig.Query> xmlQueries;

        public Parser(YdbService ydb, XmlConfig.Cdc cdc, Map<String, XmlConfig.Query> xmlQueries) {
            this.ydb = ydb;
            this.cdc = cdc;
            this.xmlQueries = xmlQueries;
        }

        @SuppressWarnings("null")
        public Result<Supplier<CdcMsgParser>> parse() {
            String changefeed = ydb.expandPath(cdc.getChangefeed());

            int index = changefeed.lastIndexOf("/");
            if (index <= 0) {
                return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                        "Changefeed name have to contain table name with  / " + changefeed, Issue.Severity.ERROR
                )));
            }

            Result<TableDescription> descRes = ydb.describeTable(changefeed.substring(0, index));
            if (!descRes.isSuccess()) {
                logger.error("Can't describe table for changefeed {}, got status {}", changefeed, descRes.getStatus());
                return descRes.map(null);
            }
            TableDescription description = descRes.getValue();

            Result<Supplier<YqlQuery>> updateQuery = findUpdateQuery(description);
            if (!updateQuery.isSuccess()) {
                return updateQuery.map(null);
            }

            Result<Supplier<YqlQuery>> deleteQuery = findDeleteQuery(description);
            if (!deleteQuery.isSuccess()) {
                return deleteQuery.map(null);
            }

            return Result.success(() -> new CdcMsgParser(updateQuery.getValue(), deleteQuery.getValue()));
        }

        private Result<Supplier<YqlQuery>> findUpdateQuery(TableDescription source) {
            if (cdc.getQuery() != null && !cdc.getQuery().trim().isEmpty()) {
                return validate(source, new XmlConfig.Query(cdc.getQuery().trim()), false);
            }
            String queryId = cdc.getUpdateQueryId();
            if (queryId != null && xmlQueries.containsKey(queryId)) {
                XmlConfig.Query query = xmlQueries.get(queryId);
                if (query.getText() != null && !query.getText().trim().isEmpty()) {
                    return validate(source, query, false);
                }
            }

            return Result.success(YqlQuery.skipMessages("update", "updateQueryId", source.getPrimaryKeys(), cdc));
        }

        private Result<Supplier<YqlQuery>> findDeleteQuery(TableDescription source) {
            String queryId = cdc.getDeleteQueryId();
            if (queryId != null && xmlQueries.containsKey(queryId)) {
                XmlConfig.Query query = xmlQueries.get(queryId);
                if (query.getText() != null && !query.getText().trim().isEmpty()) {
                    return validate(source, query, true);
                }
            }

            return Result.success(YqlQuery.skipMessages("erase", "deleteQueryId",  source.getPrimaryKeys(), cdc));
        }

        @SuppressWarnings("null")
        private Result<Supplier<YqlQuery>> validate(TableDescription source, XmlConfig.Query query, boolean keysOnly) {
            String text = query.getText().trim();
            Result<DataQuery> parsed = ydb.parseQuery(text);
            if (!parsed.isSuccess()) {
                logger.error("Can't parse query for consumer {}, got status {}", cdc.getConsumer(), parsed.getStatus());
                return parsed.map(null);
            }

            Map<String, Type> types = parsed.getValue().types();
            if (types.size() != 1) {
                return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                        "Expected only one parameter, but got " + String.join(",", types.keySet()), Issue.Severity.ERROR
                )));
            }

            String paramName = types.keySet().iterator().next();
            Type listType = types.values().iterator().next();
            if (listType.getKind() != Type.Kind.LIST) {
                return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                        "Expected type List<Struct<...>>, but got " + listType, Issue.Severity.ERROR
                )));
            }

            Type itemType = ((ListType) listType).getItemType();
            if (itemType.getKind() != Type.Kind.STRUCT) {
                return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                        "Expected type List<Struct<...>>, but got " + listType, Issue.Severity.ERROR
                )));
            }

            StructType structType = (StructType) itemType;
            Map<String, Type> tableTypes = new HashMap<>();
            for (TableColumn column: source.getColumns()) {
                tableTypes.put(column.getName(), column.getType());
            }
            Set<String> tableKeys = new HashSet();
            for (String key: source.getPrimaryKeys()) {
                tableKeys.add(key);
            }

            for (int idx = 0; idx < structType.getMembersCount(); idx += 1) {
                String name = structType.getMemberName(idx);
                Type type = structType.getMemberType(idx);
                if (!tableTypes.containsKey(name)) {
                    return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                            "Source table doesn't have column " + name, Issue.Severity.ERROR
                    )));
                }

                if (!type.equals(tableTypes.get(name))) {
                    return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                            "Source table column " + name + " has type " + tableTypes.get(name) + " instead of " + type,
                            Issue.Severity.ERROR
                    )));
                }

                if (keysOnly && !tableKeys.contains(name)) {
                    return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                            "Source table column " + name + " is not primary key and cannot be used for delete actions",
                            Issue.Severity.ERROR
                    )));
                }
            }

            List<String> keys = source.getPrimaryKeys();
            if (query.getActionTable() != null && !query.getActionTable().trim().isEmpty()) {
                String actionTable = query.getActionTable().trim();
                String action = query.getActionMode();
                if ("upsertInto".equalsIgnoreCase(action)) {
                    String execute = "UPSERT INTO `" + actionTable + "` ";
                    return Result.success(YqlQuery.readAndExecuteYql(text, execute, keys, paramName, structType, cdc));
                }
                if ("deleteFrom".equalsIgnoreCase(action)) {
                    String execute = "DELETE FROM `" + actionTable + "` ON ";
                    return Result.success(YqlQuery.readAndExecuteYql(text, execute, keys, paramName, structType, cdc));
                }
                if ("updateOn".equalsIgnoreCase(action)) {
                    String execute = "UPDATE `" + actionTable + "` ON ";
                    return Result.success(YqlQuery.readAndExecuteYql(text, execute, keys, paramName, structType, cdc));
                }
                if ("insertInto".equalsIgnoreCase(action)) {
                    String execute = "INSERT INTO `" + actionTable + "` ";
                    return Result.success(YqlQuery.readAndExecuteYql(text, execute, keys, paramName, structType, cdc));
                }

                return Result.fail(Status.of(StatusCode.CLIENT_INTERNAL_ERROR, Issue.of(
                        "Uknown actionName " + action + ", expected upsertInto/deleteFrom/updateOn/insertInto",
                        Issue.Severity.ERROR
                )));
            }

            return Result.success(YqlQuery.executeYql(text, keys, paramName, structType, cdc));
        }
    }


}