# Clickhouse Receiver

This receiver stores the metrics received from pgwatch into ClickHouse DB.

## Functionality

* Stores data in ClickHouse in `Measurements` table with the following schema:

```SQL
-- If JSON type allowed
CREATE TABLE IF NOT EXISTS Measurements(dbname String, custom_tags Map(String, String), data JSON, timestamp DateTime DEFAULT now(), PRIMARY KEY (dbname, timestamp))
-- If JSON type not allowed it falls back to storing data as String

-- To use JSON type please ensure that either you are on the latest version of clickhouse or your have allow_experimental_object_type=1
```

## Usage


```bash
export CLICKHOUSE_USER=<username>
export CLIEKHOUSE_PASSWORD=<passwd>
export CLICKHOUSE_DB=<dbname>
export CLICKHOUSE_SERVER_URI=<clickhouse_server_uri: NATIVE PORT> # Please check that you are using the native port and not the http port as the receiver is configured to utilize the native port
```

**Example:**

```bash
go build .
./clickhouse_receiver --port=8080
```

This will start the server listening on port 8080 and store received metrics in your clickhouse db instance