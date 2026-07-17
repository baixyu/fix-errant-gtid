# fix_errant_gtid

`fix_errant_gtid` compares GTID sets between a new master and an old master, streams the missing GTID transactions from the old master's binlog, converts row events into SQL statements, and writes them to a SQL file.

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
