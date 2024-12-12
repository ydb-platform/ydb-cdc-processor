## YDB Async replication app

### Build with Maven
#### Requirements

* Java 17 or newer
* Maven 3.0.0 or newer

#### Build one-in-all JAR

```
mvn clean package spring-boot:repackage
```

### Usage

Create table

```sql
CREATE TABLE table_source (
    event_time Text NOT NULL,
    event_type Text NOT NULL,
    product_id Uint32 NOT NULL,
    category_id Uint64 NOT NULL,
    category_code Text,
    brand Text,
    price Double NOT NULL,
    user_id Uint32 NOT NULL,
    user_session Text NOT NULL,
    PRIMARY KEY (product_id, category_id, user_id, user_session)
);
```

Create dependended tables

```sql
CREATE TABLE mat_view1 (
    event_time Timestamp NOT NULL, -- event_time with other type
    event_type Text NOT NULL,
    product_id Uint32 NOT NULL,
    user_id Uint32 NOT NULL,
    category_id Uint64 NOT NULL,
    user_session Text NOT NULL,
    PRIMARY KEY (product_id, category_id, user_id, user_session)
);

CREATE TABLE mat_view2 (
    user_session Text NOT NULL,
    user_id Uint32 NOT NULL,
    event_type Text NOT NULL,
    category_id Uint64 NOT NULL,
    category_code Text,
    brand Text,
    price Double NOT NULL,
    PRIMARY KEY (user_session, user_id, event_type, category_id) -- other primary key
);

```

Add changefeed to source table and consumers for every dependent table
```sql
ALTER TABLE table_source ADD CHANGEFEED cdc_topic WITH (
    FORMAT = 'JSON',
    MODE = 'NEW_IMAGE',
    INITIAL_SCAN = true,
    VIRTUAL_TIMESTAMPS = true
);

ALTER TOPIC `table_source/cdc_topic`
    ADD CONSUMER v1_consumer,
    ADD CONSUMER v2_consumer;
```


Create config file with content
```xml
<?xml version="1.0" encoding="UTF-8"?>
<config>
    <cdc changefeed="table_source/cdc_topic" consumer="v1_consumer">
<![CDATA[
DECLARE $rows AS List<Struct<
    event_time: Text,
    event_type: Text,
    product_id: Uint32,
    category_id: Uint64,
    category_code: Text?,
    user_id: Uint32,
    user_session: Text
>>;

$parse=DateTime::Parse('%Y-%m-%d %H:%M:%S %Z');

UPSERT INTO mat_view1 SELECT
    Unwrap(DateTime::MakeTimestamp($parse(event_time))) AS event_time,
    event_type,
    product_id,
    user_id,
    category_id,
    user_session
FROM AS_TABLE($rows);
]]>
    </cdc>
    <cdc changefeed="table_source/cdc_topic" consumer="v2_consumer">
<![CDATA[
DECLARE $rows AS List<Struct<
    event_type: Text,
    category_id: Uint64,
    category_code: Text?,
    brand: Text?,
    price: Double,
    user_id: Uint32,
    user_session: Text
>>;
UPSERT INTO mat_view2 SELECT * FROM AS_TABLE($rows);
]]>
    </cdc>
</config>
```

And run application
```
java -jar ydb-cdc-view-0.9.0-SNAPSHOT.jar --ydb.connection.url=<connection-url>  <path-to-config.xml>
```


