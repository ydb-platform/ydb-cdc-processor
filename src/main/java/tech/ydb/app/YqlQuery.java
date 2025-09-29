package tech.ydb.app;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Status;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.NullValue;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public abstract class YqlQuery {
    private static final Logger logger = LoggerFactory.getLogger(YqlWriter.class); // use logger of YdlWriter

    private final long batchSize;
    private final StructType structType;
    private final Map<String, Integer> keyColumns = new HashMap<>();

    protected final List<Value<?>> batch = new ArrayList<>();

    protected YqlQuery(StructType type, List<String> keys, long batchSize) {
        this.batchSize = batchSize;
        this.structType = type;

        for (int keyIndex = 0; keyIndex < keys.size(); keyIndex += 1) {
            keyColumns.put(keys.get(keyIndex), keyIndex);
        }
    }

    public void addMessage(JsonNode key, JsonNode update) throws IOException {
        Value<?>[] members = new Value<?>[structType.getMembersCount()];
        for (int idx = 0; idx < structType.getMembersCount(); idx += 1) {
            String name = structType.getMemberName(idx);
            Type type = structType.getMemberType(idx);
            if (keyColumns.containsKey(name)) {
                Integer keyIndex = keyColumns.get(name);
                members[idx] = readValue(key.get(keyIndex), type);
            } else {
                members[idx] = readValue(update != null ? update.get(name) : null, type);
            }
        }

        batch.add(structType.newValueUnsafe(members));
    }

    public boolean isFull() {
        return batch.size() >= batchSize;
    }

    public boolean isEmpty() {
        return batch.isEmpty();
    }

    public int batchSize() {
        return batch.size();
    }

    public void clear() {
        batch.clear();
    }

    public abstract Status execute(YdbService ydb);

    private Value<?> readValue(JsonNode node, Type type) throws IOException {
        if (type.getKind() == Type.Kind.OPTIONAL) {
            OptionalType optional = (OptionalType) type;
            if (node == null || node.isNull()) {
                return optional.emptyValue();
            } else {
                return readValue(node, optional.getItemType()).makeOptional();
            }
        }

        if (type.getKind() == Type.Kind.DECIMAL) {
            DecimalType decimal = (DecimalType) type;
            return decimal.newValue(node.asText());
        }

        if (type.getKind() == Type.Kind.PRIMITIVE) {
            PrimitiveType primitive = (PrimitiveType) type;
            switch (primitive) {
                case Bool:
                    return PrimitiveValue.newBool(node.asBoolean());

                case Int8:
                    return PrimitiveValue.newInt8((byte) node.asInt());
                case Int16:
                    return PrimitiveValue.newInt16((short) node.asInt());
                case Int32:
                    return PrimitiveValue.newInt32(node.asInt());
                case Int64:
                    return PrimitiveValue.newInt64(node.asLong());

                case Uint8:
                    return PrimitiveValue.newUint8(node.asInt());
                case Uint16:
                    return PrimitiveValue.newUint16(node.asInt());
                case Uint32:
                    return PrimitiveValue.newUint32(node.asLong());
                case Uint64:
                    return PrimitiveValue.newUint64(node.asLong());

                case Float:
                    return PrimitiveValue.newFloat(Double.valueOf(node.asDouble()).floatValue());
                case Double:
                    return PrimitiveValue.newDouble(node.asDouble());

                case Text:
                    return PrimitiveValue.newText(node.asText());
                case Json:
                    return PrimitiveValue.newJson(node.toString());
                case Bytes:
                    return PrimitiveValue.newBytes(Base64.getDecoder().decode(node.asText()));
                case Yson:
                    logger.warn("type YSON is not supported, ignored value {}", node.toString());
                    return PrimitiveValue.newYson("{}".getBytes());
                case JsonDocument:
                    return PrimitiveValue.newJsonDocument(node.toString());
                case Uuid:
                    return PrimitiveValue.newUuid(node.asText());
                case Date:
                    return PrimitiveValue.newDate(Instant.parse(node.asText()).atOffset(ZoneOffset.UTC).toLocalDate());
                case Datetime:
                    return PrimitiveValue.newDatetime(Instant.parse(node.asText()).atOffset(ZoneOffset.UTC).toLocalDateTime());
                case Timestamp:
                    return PrimitiveValue.newTimestamp(Instant.parse(node.asText()));
                case Interval:
                    return PrimitiveValue.newInterval(Duration.ofSeconds(node.asLong()));
                case TzDate:
                case TzTimestamp:
                case TzDatetime:
                case DyNumber:
                default:
                    break;
            }
        }

        logger.warn("unsupported type {}", type);
        throw new IOException("Can't read node value " + node + " with type " + type);
    }

    public static Supplier<YqlQuery> skipMessages(String type, String config, List<String> keys, XmlConfig.Cdc xml) {
        final int batchSize = xml.getBatchSize();
        return () -> new YqlQuery(null, keys, batchSize) {
            @Override
            public void addMessage(JsonNode key, JsonNode update) throws IOException {
                batch.add(NullValue.of());
            }

            @Override
            public Status execute(YdbService ydb) {
                logger.info("skipped {} {} messages because {} is not configured for changefeed {}, consumer {}",
                        batch.size(), type, config, xml.getChangefeed(), xml.getConsumer());
                return Status.SUCCESS;
            }
        };
    }

    public static Supplier<YqlQuery> executeYql(String query, List<String> keys, String name, StructType type, XmlConfig.Cdc config) {
        final int batchSize = config.getBatchSize();
        final int timeout = config.getTimeoutSeconds();
        return () -> new YqlQuery(type, keys, batchSize) {
            @Override
            public Status execute(YdbService ydb) {
                Params prm = Params.of(name, ListType.of(type).newValue(batch));
                return ydb.executeYqlQuery(query, prm, timeout);
            }
        };
    }
}
