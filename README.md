# fix_errant_gtid

`fix_errant_gtid` compares GTID sets between a new master and an old master, streams the missing GTID transactions from the old master's binlog, converts row events into SQL statements, and writes them to a SQL file.

Typical use case: an asynchronous MySQL replication topology performs an abnormal failover, and the promoted new master is missing some committed transactions that still exist in the old master's binlogs. The tool identifies those old-master-only GTID transactions and reconstructs equivalent row-level SQL that can be reviewed and applied to repair the new master.

## Build

```bash
go build ./cmd/fix-errant-gtid
```

## Usage

```bash
./fix-errant-gtid -config config.json -out repair.sql new_master_ip  old_master_ip
./fix-errant-gtid -config config.json -out repair.sql 10.0.0.11 10.0.0.12
```

The two positional arguments are only the new master IP and old master IP. Authentication and port are read from the JSON config file.

Example config:

```json
{
  "mysql": {
    "user": "repl",
    "password": "password",
    "port": 3306,
    "params": {
      "charset": "utf8mb4"
    }
  },
  "new_master": {},
  "old_master": {
    "password": "old-master-password"
  }
}
```

`mysql` is the shared default. `new_master` and `old_master` can override `user`, `password`, `port`, `network`, and `params` when the two servers use different credentials. The positional arguments must not include a port; put the port in the config file.

The old master account needs enough privilege to read metadata and stream binlogs, typically `REPLICATION SLAVE`/`REPLICATION CLIENT` plus `SELECT` on `information_schema`.

## GTID Snapshot and Streaming

The tool reads `@@GLOBAL.gtid_executed` from both servers before calculating errant transactions. To avoid comparing inconsistent GTID snapshots while replication or writes are still moving, it reads both servers twice:

```text
read new master gtid_executed
read old master gtid_executed
read new master gtid_executed again
read old master gtid_executed again
```

If either server's GTID set changes between the two reads, that attempt is discarded and retried. The tool retries 3 times. If the GTID sets are still changing after those retries, it exits with an error instead of generating SQL from an unstable view of the topology.

After a stable snapshot is found, errant GTIDs are calculated as:

```text
errant = old_master.gtid_executed - new_master.gtid_executed
```

The binlog stream connects to the old master. The GTID set passed to MySQL GTID auto-position is not the errant set itself; it is the old master's executed set with the errant transactions removed:

```text
stream_start = old_master.gtid_executed - errant
```

This tells the old master which of its own GTIDs the tool already has, so the stream returns the missing errant transactions. It also avoids sending GTIDs that exist only on the new master to the old master.

For the most reliable result, run the tool when the old master is no longer accepting writes and the replication relationship between the old and new masters is stable. If the old master is still receiving the new master's transactions, or either server's `gtid_executed` is actively changing, the stable snapshot check may retry and eventually fail.

## MySQL Compatibility

The generated SQL uses syntax compatible with MySQL 5.7 and MySQL 8.0:

- Identifiers are always quoted with backticks, including words that are reserved in MySQL 8.0.
- Text values avoid backslash escapes; backslashes and control bytes are emitted with `CONCAT(..., CHAR(...), ...)` so the SQL is stable with or without `NO_BACKSLASH_ESCAPES`.
- Binary values are emitted as hex literals, for example `0x00ABFF`.
- The output starts with `SET NAMES utf8mb4;`.

## Type Reconstruction

The tool combines binlog row values with `information_schema.COLUMNS` metadata to reconstruct values more accurately:

- Unsigned integer columns are converted back to unsigned values. For example, a `TINYINT UNSIGNED` value of `200` may be decoded by the binlog library as `int8(-56)`, and the tool emits `200`.
- `BINARY`, `VARBINARY`, and `BLOB` family columns are emitted as hex literals, for example `0x68656C6C6F313233`.
- `CHAR`, `VARCHAR`, `TEXT` family columns, and `JSON` are emitted as text literals with MySQL-compatible escaping.
- `ENUM` and `SET` columns are restored from their numeric binlog representation using `COLUMN_TYPE`, for example `'male'` and `'read,admin'`.
- `DECIMAL` values are emitted as numeric literals, not quoted strings.
- Spatial columns are emitted as equivalent WKB-based expressions such as `ST_GeomFromWKB(0x...)`. Row binlogs do not preserve the original function call text, so an original `ST_GeomFromText(...)` insert cannot be reproduced byte-for-byte as the same SQL expression.

The tool assumes row-based binlogs. It emits `INSERT` statements for `WRITE_ROWS` events, `UPDATE ... SET ... WHERE ... LIMIT 1` statements for `UPDATE_ROWS` events, and `DELETE ... WHERE ... LIMIT 1` statements for `DELETE_ROWS` events. Row-based binlogs do not preserve the original SQL text, so generated update and delete statements are equivalent row-level SQL reconstructed from before and after row images.

For complete reconstructed rows, the old master should have `binlog_row_image=FULL`. If the binlog contains partial row images, columns not present in the row event are omitted from generated `INSERT` column lists, `UPDATE` assignments or predicates, and `DELETE` predicates.
