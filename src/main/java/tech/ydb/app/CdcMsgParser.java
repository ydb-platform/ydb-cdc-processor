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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveType;
import static tech.ydb.table.values.PrimitiveType.Bool;
import static tech.ydb.table.values.PrimitiveType.Bytes;
import static tech.ydb.table.values.PrimitiveType.Date;
import static tech.ydb.table.values.PrimitiveType.Datetime;
import static tech.ydb.table.values.PrimitiveType.DyNumber;
import static tech.ydb.table.values.PrimitiveType.Float;
import static tech.ydb.table.values.PrimitiveType.Int16;
import static tech.ydb.table.values.PrimitiveType.Int32;
import static tech.ydb.table.values.PrimitiveType.Int64;
import static tech.ydb.table.values.PrimitiveType.Int8;
import static tech.ydb.table.values.PrimitiveType.Interval;
import static tech.ydb.table.values.PrimitiveType.Json;
import static tech.ydb.table.values.PrimitiveType.JsonDocument;
import static tech.ydb.table.values.PrimitiveType.Text;
import static tech.ydb.table.values.PrimitiveType.Timestamp;
import static tech.ydb.table.values.PrimitiveType.TzDate;
import static tech.ydb.table.values.PrimitiveType.TzDatetime;
import static tech.ydb.table.values.PrimitiveType.TzTimestamp;
import static tech.ydb.table.values.PrimitiveType.Uint16;
import static tech.ydb.table.values.PrimitiveType.Uint32;
import static tech.ydb.table.values.PrimitiveType.Uint64;
import static tech.ydb.table.values.PrimitiveType.Uint8;
import static tech.ydb.table.values.PrimitiveType.Uuid;
import static tech.ydb.table.values.PrimitiveType.Yson;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class CdcMsgParser {
    private static final int WRITE_BATCH_SIZE = 1000;
    private static final Logger logger = LoggerFactory.getLogger(YqlWriter.class);

    private final ObjectMapper mapper = new ObjectMapper();

    private final String paramName;
    private final StructType structType;
    private final Map<String, Integer> keyColumns = new HashMap<>();

    private final List<Value<?>> batch = new ArrayList<>();

    public CdcMsgParser(String paramName, StructType type, TableDescription desc) {
        this.paramName = paramName;
        this.structType = type;

        for (int keyIndex = 0; keyIndex < desc.getPrimaryKeys().size(); keyIndex += 1) {
            keyColumns.put(desc.getPrimaryKeys().get(keyIndex), keyIndex);
        }
    }

    public boolean isFull() {
        return batch.size() >= WRITE_BATCH_SIZE;
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

    public Params build() {
        ListValue value = ListType.of(structType).newValue(batch);
        return Params.of(paramName, value);
    }

    public void addMessage(byte[] json) throws IOException {
        JsonNode root = mapper.readTree(json);
        if (!root.isObject() || !root.hasNonNull("update") || !root.hasNonNull("newImage") || !root.hasNonNull("key")) {
            logger.error("unsupported cdc message {}", new String(json));
            return;
        }

        JsonNode newImage = root.get("newImage");
        JsonNode key = root.get("key");

        if (!newImage.isObject() || !key.isArray()) {
            logger.error("unsupported cdc message {}", new String(json));
            return;
        }

        Value<?>[] members = new Value<?>[structType.getMembersCount()];
        for (int idx = 0; idx < structType.getMembersCount(); idx += 1) {
            String name = structType.getMemberName(idx);
            Type type = structType.getMemberType(idx);
            if (keyColumns.containsKey(name)) {
                Integer keyIndex = keyColumns.get(name);
                members[idx] = readValue(key.get(keyIndex), type);
            } else {
                members[idx] = readValue(newImage.get(name), type);
            }
        }

        batch.add(structType.newValueUnsafe(members));
    }

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
}
