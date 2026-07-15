package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"
)

type config struct {
	NewMasterIP string
	OldMasterIP string
	ConfigPath  string
	OutPath     string
	ServerID    uint
	Flavor      string
	ReadTimeout time.Duration
}

type fileConfig struct {
	MySQL     mysqlAuthConfig `json:"mysql"`
	NewMaster mysqlAuthConfig `json:"new_master"`
	OldMaster mysqlAuthConfig `json:"old_master"`
}

type mysqlAuthConfig struct {
	User     string            `json:"user"`
	Password string            `json:"password"`
	Port     int               `json:"port"`
	Net      string            `json:"network"`
	Params   map[string]string `json:"params"`
}

type tableName struct {
	Schema string
	Table  string
}

type tableSchema struct {
	Columns []columnSchema
}

type columnSchema struct {
	Name             string
	DataType         string
	ColumnType       string
	CharacterSetName string
	Unsigned         bool
	Binary           bool
	Text             bool
	EnumValues       []string
	SetValues        []string
	Geometry         bool
}

type tableSchemaCache struct {
	db    *sql.DB
	cache map[tableName]tableSchema
}

type app struct {
	cfg        config
	newDB      *sql.DB
	oldDB      *sql.DB
	oldDSN     *mysql.Config
	out        io.Writer
	schema     *tableSchemaCache
	errant     *gomysql.MysqlGTIDSet
	pending    map[string]struct{}
	current    string
	inWanted   bool
	statements int
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if err := run(context.Background(), cfg); err != nil {
		log.Fatal(err)
	}
}

func parseConfig(args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("fix-errant-gtid", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.ConfigPath, "config", "config.json", "JSON config file containing MySQL authentication")
	fs.StringVar(&cfg.OutPath, "out", "errant_gtid_repair.sql", "output SQL file")
	fs.UintVar(&cfg.ServerID, "server-id", 0, "replication server ID used when streaming binlogs")
	fs.StringVar(&cfg.Flavor, "flavor", "mysql", "binlog flavor: mysql or mariadb")
	fs.DurationVar(&cfg.ReadTimeout, "read-timeout", 30*time.Second, "timeout while waiting for old master binlog events")
	if err := fs.Parse(args); err != nil {
		return cfg, usageError(err)
	}
	if fs.NArg() != 2 {
		return cfg, usageError(errors.New("expected exactly 2 positional arguments: <new-master-ip> <old-master-ip>"))
	}
	cfg.NewMasterIP = fs.Arg(0)
	cfg.OldMasterIP = fs.Arg(1)
	if cfg.ServerID == 0 {
		cfg.ServerID = uint(100000 + rand.New(rand.NewSource(time.Now().UnixNano())).Intn(899999))
	}
	return cfg, nil
}

func usageError(err error) error {
	return fmt.Errorf(`%w

Usage:
  fix-errant-gtid [flags] <new-master-ip> <old-master-ip>

Flags:
  -config string
        JSON config file containing MySQL authentication (default "config.json")
  -out string
        output SQL file (default "errant_gtid_repair.sql")
  -server-id uint
        replication server ID used when streaming binlogs (default random)
  -flavor string
        binlog flavor: mysql or mariadb (default "mysql")
  -read-timeout duration
        timeout while waiting for old master binlog events (default 30s)

Example:
  fix-errant-gtid -config config.json -out repair.sql 10.0.0.11 10.0.0.12`, err)
}

func loadFileConfig(path string) (fileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return fileConfig{}, fmt.Errorf("read config file %q: %w", path, err)
	}

	var cfg fileConfig
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return fileConfig{}, fmt.Errorf("parse config file %q: %w", path, err)
	}
	if cfg.MySQL.Port == 0 {
		cfg.MySQL.Port = 3306
	}
	if cfg.MySQL.Net == "" {
		cfg.MySQL.Net = "tcp"
	}
	if cfg.MySQL.User == "" {
		return fileConfig{}, fmt.Errorf("config file %q must set mysql.user", path)
	}
	if cfg.MySQL.Port < 1 || cfg.MySQL.Port > 65535 {
		return fileConfig{}, fmt.Errorf("config file %q has invalid mysql.port %d", path, cfg.MySQL.Port)
	}
	if cfg.MySQL.Net != "tcp" {
		return fileConfig{}, fmt.Errorf("config file %q uses mysql.network %q; binlog streaming requires tcp", path, cfg.MySQL.Net)
	}
	return cfg, nil
}

func buildMySQLConfig(host string, base mysqlAuthConfig, override mysqlAuthConfig) (*mysql.Config, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		return nil, errors.New("host cannot be empty")
	}
	if strings.Contains(host, "@") || strings.Contains(host, "/") {
		return nil, fmt.Errorf("expected only an IP/host argument, got %q", host)
	}
	if strings.Contains(host, ":") && net.ParseIP(host) == nil {
		return nil, fmt.Errorf("expected only an IP/host argument without port, got %q; configure the port in config.json", host)
	}

	auth := mergeAuthConfig(base, override)
	if auth.User == "" {
		return nil, errors.New("mysql user cannot be empty")
	}
	if auth.Port == 0 {
		auth.Port = 3306
	}
	if auth.Net == "" {
		auth.Net = "tcp"
	}
	if auth.Net != "tcp" {
		return nil, fmt.Errorf("network %q is not supported; binlog streaming requires tcp", auth.Net)
	}
	if auth.Port < 1 || auth.Port > 65535 {
		return nil, fmt.Errorf("invalid MySQL port %d", auth.Port)
	}

	params := make(map[string]string, len(auth.Params)+1)
	for key, value := range auth.Params {
		params[key] = value
	}
	if _, ok := params["charset"]; !ok {
		params["charset"] = "utf8mb4"
	}

	return &mysql.Config{
		User:                 auth.User,
		Passwd:               auth.Password,
		Net:                  auth.Net,
		Addr:                 net.JoinHostPort(host, strconv.Itoa(auth.Port)),
		Params:               params,
		ParseTime:            true,
		AllowNativePasswords: true,
	}, nil
}

func mergeAuthConfig(base, override mysqlAuthConfig) mysqlAuthConfig {
	merged := base
	if override.User != "" {
		merged.User = override.User
	}
	if override.Password != "" {
		merged.Password = override.Password
	}
	if override.Port != 0 {
		merged.Port = override.Port
	}
	if override.Net != "" {
		merged.Net = override.Net
	}
	if len(base.Params) > 0 || len(override.Params) > 0 {
		merged.Params = make(map[string]string, len(base.Params)+len(override.Params))
		for key, value := range base.Params {
			merged.Params[key] = value
		}
		for key, value := range override.Params {
			merged.Params[key] = value
		}
	}
	return merged
}

func run(ctx context.Context, cfg config) error {
	fileCfg, err := loadFileConfig(cfg.ConfigPath)
	if err != nil {
		return err
	}

	newDSN, err := buildMySQLConfig(cfg.NewMasterIP, fileCfg.MySQL, fileCfg.NewMaster)
	if err != nil {
		return fmt.Errorf("build new master DSN: %w", err)
	}
	oldDSN, err := buildMySQLConfig(cfg.OldMasterIP, fileCfg.MySQL, fileCfg.OldMaster)
	if err != nil {
		return fmt.Errorf("build old master DSN: %w", err)
	}
	prepareSQLDSN(newDSN)
	prepareSQLDSN(oldDSN)

	newDB, err := sql.Open("mysql", newDSN.FormatDSN())
	if err != nil {
		return fmt.Errorf("open new master: %w", err)
	}
	defer newDB.Close()
	oldDB, err := sql.Open("mysql", oldDSN.FormatDSN())
	if err != nil {
		return fmt.Errorf("open old master: %w", err)
	}
	defer oldDB.Close()

	if err := newDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping new master: %w", err)
	}
	if err := oldDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping old master: %w", err)
	}

	newExecuted, err := fetchGTIDExecuted(ctx, newDB)
	if err != nil {
		return fmt.Errorf("read new master gtid_executed: %w", err)
	}
	oldExecuted, err := fetchGTIDExecuted(ctx, oldDB)
	if err != nil {
		return fmt.Errorf("read old master gtid_executed: %w", err)
	}
	errant := subtractGTIDSets(oldExecuted, newExecuted)
	if errant.String() == "" {
		fmt.Println("No errant GTIDs found: old master has no GTID transactions missing from new master.")
		return nil
	}

	pending, err := enumerateGTIDs(errant, 100000)
	if err != nil {
		return err
	}

	out, err := os.Create(cfg.OutPath)
	if err != nil {
		return fmt.Errorf("create output file %q: %w", cfg.OutPath, err)
	}
	defer out.Close()

	a := &app{
		cfg:     cfg,
		newDB:   newDB,
		oldDB:   oldDB,
		oldDSN:  oldDSN,
		out:     out,
		schema:  &tableSchemaCache{db: oldDB, cache: make(map[tableName]tableSchema)},
		errant:  errant,
		pending: pending,
	}

	if err := a.writeHeader(newExecuted, oldExecuted); err != nil {
		return err
	}
	if err := a.streamErrantTransactions(ctx, newExecuted); err != nil {
		return err
	}
	if len(a.pending) > 0 {
		return fmt.Errorf("binlog stream ended before all errant GTIDs were parsed; missing %d GTIDs, examples: %s", len(a.pending), samplePending(a.pending, 5))
	}

	fmt.Printf("Wrote %d DML statements for %d errant GTIDs to %s\n", a.statements, len(pending), cfg.OutPath)
	return nil
}

func prepareSQLDSN(cfg *mysql.Config) {
	cfg.ParseTime = true
	cfg.MultiStatements = false
	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	if _, ok := cfg.Params["charset"]; !ok {
		cfg.Params["charset"] = "utf8mb4"
	}
}

func fetchGTIDExecuted(ctx context.Context, db *sql.DB) (*gomysql.MysqlGTIDSet, error) {
	var gtid sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&gtid); err != nil {
		return nil, err
	}
	if !gtid.Valid || strings.TrimSpace(gtid.String) == "" {
		return parseMysqlGTIDSet("")
	}
	return parseMysqlGTIDSet(gtid.String)
}

func parseMysqlGTIDSet(gtid string) (*gomysql.MysqlGTIDSet, error) {
	parsed, err := gomysql.ParseMysqlGTIDSet(gtid)
	if err != nil {
		return nil, err
	}
	set, ok := parsed.(*gomysql.MysqlGTIDSet)
	if !ok {
		return nil, fmt.Errorf("parsed GTID set is %T, expected *mysql.MysqlGTIDSet", parsed)
	}
	return set, nil
}

func subtractGTIDSets(left, right *gomysql.MysqlGTIDSet) *gomysql.MysqlGTIDSet {
	out := left.Clone().(*gomysql.MysqlGTIDSet)
	_ = out.Minus(*right)
	return out
}

func enumerateGTIDs(set *gomysql.MysqlGTIDSet, limit uint64) (map[string]struct{}, error) {
	pending := make(map[string]struct{})
	var count uint64
	for sid, uuidSet := range set.Sets {
		for _, interval := range uuidSet.Intervals {
			for gno := interval.Start; gno < interval.Stop; gno++ {
				count++
				if count > limit {
					return nil, fmt.Errorf("errant GTID count exceeds safety limit %d; refusing to enumerate", limit)
				}
				pending[formatGTID(sid, gno)] = struct{}{}
			}
		}
	}
	return pending, nil
}

func formatGTID(sid string, gno int64) string {
	return fmt.Sprintf("%s:%d", sid, gno)
}

func (a *app) writeHeader(newExecuted, oldExecuted *gomysql.MysqlGTIDSet) error {
	_, err := fmt.Fprintf(a.out, `-- Generated by fix-errant-gtid at %s
-- new master gtid_executed: %s
-- old master gtid_executed: %s
-- errant gtid set: %s
-- This file contains DML statements reconstructed from row binlog events.
-- Row binlogs do not preserve the original SQL text; DELETE and UPDATE rows are emitted as equivalent row-level statements.
-- Review the SQL before applying it.

SET NAMES utf8mb4;

`, time.Now().Format(time.RFC3339), newExecuted.String(), oldExecuted.String(), a.errant.String())
	return err
}

func (a *app) streamErrantTransactions(ctx context.Context, start *gomysql.MysqlGTIDSet) error {
	host, port, err := replicationEndpoint(a.oldDSN)
	if err != nil {
		return err
	}

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:        uint32(a.cfg.ServerID),
		Flavor:          a.cfg.Flavor,
		Host:            host,
		Port:            uint16(port),
		User:            a.oldDSN.User,
		Password:        a.oldDSN.Passwd,
		Charset:         "utf8mb4",
		ReadTimeout:     a.cfg.ReadTimeout,
		UseDecimal:      true,
		ParseTime:       true,
		SemiSyncEnabled: false,
	})
	defer syncer.Close()

	streamer, err := syncer.StartSyncGTID(start)
	if err != nil {
		return fmt.Errorf("start GTID binlog sync from old master: %w", err)
	}

	for len(a.pending) > 0 {
		eventCtx, cancel := context.WithTimeout(ctx, a.cfg.ReadTimeout)
		ev, err := streamer.GetEvent(eventCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("read binlog event from old master: %w", err)
		}
		if err := a.handleEvent(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

func replicationEndpoint(cfg *mysql.Config) (string, int, error) {
	if cfg.Net != "" && cfg.Net != "tcp" {
		return "", 0, fmt.Errorf("old master DSN uses network %q; binlog streaming currently requires tcp(host:port)", cfg.Net)
	}
	addr := cfg.Addr
	if addr == "" {
		addr = "127.0.0.1:3306"
	}
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		if strings.Count(addr, ":") == 0 {
			return addr, 3306, nil
		}
		return "", 0, fmt.Errorf("parse old master address %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("invalid old master port %q", portText)
	}
	return host, port, nil
}

func (a *app) handleEvent(ctx context.Context, ev *replication.BinlogEvent) error {
	switch e := ev.Event.(type) {
	case *replication.GTIDEvent:
		gtidSet, err := e.GTIDNext()
		if err != nil {
			return fmt.Errorf("parse GTID event: %w", err)
		}
		gtid := gtidSet.String()
		a.current = strings.TrimSpace(gtid)
		_, a.inWanted = a.pending[a.current]
		if a.inWanted {
			_, err := fmt.Fprintf(a.out, "\n-- GTID %s\n", a.current)
			return err
		}
	case *replication.XIDEvent:
		if a.inWanted {
			delete(a.pending, a.current)
			a.inWanted = false
			a.current = ""
		}
	case *replication.QueryEvent:
		if a.inWanted && strings.EqualFold(strings.TrimSpace(string(e.Query)), "COMMIT") {
			delete(a.pending, a.current)
			a.inWanted = false
			a.current = ""
		}
	case *replication.RowsEvent:
		if !a.inWanted {
			return nil
		}
		return a.writeRowsEvent(ctx, ev.Header.EventType, e)
	}
	return nil
}

func (a *app) writeRowsEvent(ctx context.Context, eventType replication.EventType, ev *replication.RowsEvent) error {
	name := tableName{
		Schema: string(ev.Table.Schema),
		Table:  string(ev.Table.Table),
	}
	schema, err := a.schema.get(ctx, name)
	if err != nil {
		return err
	}

	switch eventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		for i, row := range ev.Rows {
			if err := a.writeInsert(name, schema, row, skippedColumns(ev, i)); err != nil {
				return err
			}
		}
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		if len(ev.Rows)%2 != 0 {
			return fmt.Errorf("update rows event for %s.%s has %d row images; expected before/after pairs", name.Schema, name.Table, len(ev.Rows))
		}
		for i := 0; i < len(ev.Rows); i += 2 {
			if err := a.writeUpdate(name, schema, ev.Rows[i], ev.Rows[i+1], skippedColumns(ev, i), skippedColumns(ev, i+1)); err != nil {
				return err
			}
		}
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		for i, row := range ev.Rows {
			if err := a.writeDelete(name, schema, row, skippedColumns(ev, i)); err != nil {
				return err
			}
		}
	default:
		_, err := fmt.Fprintf(a.out, "-- Unsupported row event type %s on %s.%s skipped\n", eventType.String(), quoteIdent(name.Schema), quoteIdent(name.Table))
		return err
	}
	return nil
}

func skippedColumns(ev *replication.RowsEvent, rowIndex int) map[int]struct{} {
	out := make(map[int]struct{})
	if rowIndex >= len(ev.SkippedColumns) {
		return out
	}
	for _, columnIndex := range ev.SkippedColumns[rowIndex] {
		out[columnIndex] = struct{}{}
	}
	return out
}

func (a *app) writeInsert(name tableName, schema tableSchema, row []interface{}, skipped map[int]struct{}) error {
	if err := validateRowLength(name, schema, row); err != nil {
		return err
	}
	columns := make([]string, 0, len(row))
	values := make([]string, 0, len(row))
	for i, value := range row {
		if isSkipped(skipped, i) {
			continue
		}
		column := schema.Columns[i]
		columns = append(columns, column.Name)
		values = append(values, sqlLiteralForColumn(value, column))
	}
	if len(columns) == 0 {
		_, err := fmt.Fprintf(a.out, "-- Row on %s.%s skipped: no columns were present in the row image\n", quoteIdent(name.Schema), quoteIdent(name.Table))
		return err
	}
	_, err := fmt.Fprintf(
		a.out,
		"INSERT INTO %s.%s (%s) VALUES (%s);\n",
		quoteIdent(name.Schema),
		quoteIdent(name.Table),
		quoteIdentList(columns),
		strings.Join(values, ", "),
	)
	if err == nil {
		a.statements++
	}
	return err
}

func (a *app) writeDelete(name tableName, schema tableSchema, row []interface{}, skipped map[int]struct{}) error {
	where, err := rowWhereClause(name, schema, row, skipped)
	if err != nil {
		return err
	}
	if where == "" {
		_, err := fmt.Fprintf(a.out, "-- DELETE_ROWS on %s.%s skipped: no columns were present in the before image\n", quoteIdent(name.Schema), quoteIdent(name.Table))
		return err
	}

	_, err = fmt.Fprintf(
		a.out,
		"DELETE FROM %s.%s WHERE %s LIMIT 1;\n",
		quoteIdent(name.Schema),
		quoteIdent(name.Table),
		where,
	)
	if err == nil {
		a.statements++
	}
	return err
}

func (a *app) writeUpdate(name tableName, schema tableSchema, beforeRow, afterRow []interface{}, beforeSkipped, afterSkipped map[int]struct{}) error {
	where, err := rowWhereClause(name, schema, beforeRow, beforeSkipped)
	if err != nil {
		return err
	}
	if where == "" {
		_, err := fmt.Fprintf(a.out, "-- UPDATE_ROWS on %s.%s skipped: no columns were present in the before image\n", quoteIdent(name.Schema), quoteIdent(name.Table))
		return err
	}

	assignments, err := updateAssignments(name, schema, beforeRow, afterRow, beforeSkipped, afterSkipped)
	if err != nil {
		return err
	}
	if len(assignments) == 0 {
		_, err := fmt.Fprintf(a.out, "-- UPDATE_ROWS on %s.%s skipped: no columns were present in the after image\n", quoteIdent(name.Schema), quoteIdent(name.Table))
		return err
	}

	_, err = fmt.Fprintf(
		a.out,
		"UPDATE %s.%s SET %s WHERE %s LIMIT 1;\n",
		quoteIdent(name.Schema),
		quoteIdent(name.Table),
		strings.Join(assignments, ", "),
		where,
	)
	if err == nil {
		a.statements++
	}
	return err
}

func rowWhereClause(name tableName, schema tableSchema, row []interface{}, skipped map[int]struct{}) (string, error) {
	if err := validateRowLength(name, schema, row); err != nil {
		return "", err
	}

	conditions := make([]string, 0, len(row))
	for i, value := range row {
		if isSkipped(skipped, i) {
			continue
		}
		column := schema.Columns[i]
		if value == nil {
			conditions = append(conditions, fmt.Sprintf("%s IS NULL", quoteIdent(column.Name)))
			continue
		}
		conditions = append(conditions, fmt.Sprintf("%s = %s", quoteIdent(column.Name), sqlLiteralForColumn(value, column)))
	}
	return strings.Join(conditions, " AND "), nil
}

func updateAssignments(name tableName, schema tableSchema, beforeRow, afterRow []interface{}, beforeSkipped, afterSkipped map[int]struct{}) ([]string, error) {
	if err := validateRowLength(name, schema, beforeRow); err != nil {
		return nil, err
	}
	if err := validateRowLength(name, schema, afterRow); err != nil {
		return nil, err
	}

	all := make([]string, 0, len(afterRow))
	changed := make([]string, 0, len(afterRow))
	for i, value := range afterRow {
		if isSkipped(afterSkipped, i) {
			continue
		}
		column := schema.Columns[i]
		afterLiteral := sqlLiteralForColumn(value, column)
		assignment := fmt.Sprintf("%s = %s", quoteIdent(column.Name), afterLiteral)
		all = append(all, assignment)

		if i >= len(beforeRow) || isSkipped(beforeSkipped, i) {
			changed = append(changed, assignment)
			continue
		}
		if sqlLiteralForColumn(beforeRow[i], column) != afterLiteral {
			changed = append(changed, assignment)
		}
	}
	if len(changed) > 0 {
		return changed, nil
	}
	return all, nil
}

func validateRowLength(name tableName, schema tableSchema, row []interface{}) error {
	if len(row) > len(schema.Columns) {
		return fmt.Errorf("row for %s.%s has %d values, but table metadata has %d columns", name.Schema, name.Table, len(row), len(schema.Columns))
	}
	return nil
}

func isSkipped(skipped map[int]struct{}, index int) bool {
	_, ok := skipped[index]
	return ok
}

func (c *tableSchemaCache) get(ctx context.Context, name tableName) (tableSchema, error) {
	if schema, ok := c.cache[name]; ok {
		return schema, nil
	}
	rows, err := c.db.QueryContext(ctx, `
SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, COALESCE(CHARACTER_SET_NAME, '')
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION`, name.Schema, name.Table)
	if err != nil {
		return tableSchema{}, fmt.Errorf("read schema for %s.%s: %w", name.Schema, name.Table, err)
	}
	defer rows.Close()

	var columns []columnSchema
	for rows.Next() {
		var columnName string
		var dataType string
		var columnType string
		var characterSetName string
		if err := rows.Scan(&columnName, &dataType, &columnType, &characterSetName); err != nil {
			return tableSchema{}, err
		}
		columns = append(columns, buildColumnSchema(columnName, dataType, columnType, characterSetName))
	}
	if err := rows.Err(); err != nil {
		return tableSchema{}, err
	}
	if len(columns) == 0 {
		return tableSchema{}, fmt.Errorf("no columns found for %s.%s on old master", name.Schema, name.Table)
	}
	schema := tableSchema{Columns: columns}
	c.cache[name] = schema
	return schema, nil
}

func buildColumnSchema(name, dataType, columnType, characterSetName string) columnSchema {
	dataType = strings.ToLower(dataType)
	columnTypeLower := strings.ToLower(columnType)
	column := columnSchema{
		Name:             name,
		DataType:         dataType,
		ColumnType:       columnType,
		CharacterSetName: characterSetName,
		Unsigned:         strings.Contains(columnTypeLower, "unsigned"),
		Binary:           isBinaryDataType(dataType),
		Text:             isTextDataType(dataType),
		Geometry:         isGeometryDataType(dataType),
	}
	switch dataType {
	case "enum":
		column.EnumValues = parseEnumSetValues(columnType)
	case "set":
		column.SetValues = parseEnumSetValues(columnType)
	}
	return column
}

func isBinaryDataType(dataType string) bool {
	switch dataType {
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return true
	default:
		return false
	}
}

func isTextDataType(dataType string) bool {
	switch dataType {
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "json":
		return true
	default:
		return false
	}
}

func isGeometryDataType(dataType string) bool {
	switch dataType {
	case "geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection":
		return true
	default:
		return false
	}
}

func parseEnumSetValues(columnType string) []string {
	start := strings.IndexByte(columnType, '(')
	end := strings.LastIndexByte(columnType, ')')
	if start < 0 || end <= start {
		return nil
	}
	body := columnType[start+1 : end]
	values := make([]string, 0)
	for i := 0; i < len(body); {
		for i < len(body) && (body[i] == ' ' || body[i] == ',') {
			i++
		}
		if i >= len(body) || body[i] != '\'' {
			break
		}
		i++
		var value strings.Builder
		for i < len(body) {
			ch := body[i]
			if ch == '\'' {
				if i+1 < len(body) && body[i+1] == '\'' {
					value.WriteByte('\'')
					i += 2
					continue
				}
				i++
				break
			}
			if ch == '\\' && i+1 < len(body) {
				i++
				value.WriteByte(body[i])
				i++
				continue
			}
			value.WriteByte(ch)
			i++
		}
		values = append(values, value.String())
	}
	return values
}

func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func quoteIdentList(columns []string) string {
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, quoteIdent(column))
	}
	return strings.Join(quoted, ", ")
}

func sqlLiteralForColumn(v interface{}, column columnSchema) string {
	if v == nil {
		return "NULL"
	}
	if len(column.EnumValues) > 0 {
		return enumLiteral(v, column.EnumValues)
	}
	if len(column.SetValues) > 0 {
		return setLiteral(v, column.SetValues)
	}
	if column.Geometry {
		return geometryLiteral(v)
	}
	if column.Binary {
		return hexLiteral(valueBytes(v))
	}
	if column.Text {
		switch value := v.(type) {
		case []byte:
			return quoteString(string(value))
		case string:
			return quoteString(value)
		}
	}
	if column.Unsigned {
		if literal, ok := unsignedLiteral(v, column.DataType); ok {
			return literal
		}
	}
	return sqlLiteral(v)
}

func unsignedLiteral(v interface{}, dataType string) (string, bool) {
	n, ok := signedInt64(v)
	if !ok {
		return "", false
	}

	switch dataType {
	case "tinyint":
		return fmt.Sprintf("%d", uint8(n)), true
	case "smallint":
		return fmt.Sprintf("%d", uint16(n)), true
	case "mediumint":
		return fmt.Sprintf("%d", uint32(n)&0xFFFFFF), true
	case "int", "integer":
		return fmt.Sprintf("%d", uint32(n)), true
	case "bigint":
		return fmt.Sprintf("%d", uint64(n)), true
	default:
		return "", false
	}
}

func signedInt64(v interface{}) (int64, bool) {
	switch value := v.(type) {
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint:
		return int64(value), true
	case uint8:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		return int64(value), true
	default:
		return 0, false
	}
}

func enumLiteral(v interface{}, values []string) string {
	index, ok := signedInt64(v)
	if !ok {
		return sqlLiteral(v)
	}
	if index <= 0 || int(index) > len(values) {
		return quoteString("")
	}
	return quoteString(values[index-1])
}

func setLiteral(v interface{}, values []string) string {
	mask, ok := signedInt64(v)
	if !ok {
		return sqlLiteral(v)
	}
	var selected []string
	for i, value := range values {
		if uint64(mask)&(uint64(1)<<uint(i)) != 0 {
			selected = append(selected, value)
		}
	}
	return quoteString(strings.Join(selected, ","))
}

func geometryLiteral(v interface{}) string {
	data := valueBytes(v)
	if len(data) <= 4 {
		return hexLiteral(data)
	}
	srid := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
	wkb := data[4:]
	if srid == 0 {
		return "ST_GeomFromWKB(" + hexLiteral(wkb) + ")"
	}
	return fmt.Sprintf("ST_GeomFromWKB(%s, %d)", hexLiteral(wkb), srid)
}

func valueBytes(v interface{}) []byte {
	switch value := v.(type) {
	case []byte:
		return value
	case string:
		return []byte(value)
	case fmt.Stringer:
		return []byte(value.String())
	default:
		return []byte(fmt.Sprint(value))
	}
}

func hexLiteral(b []byte) string {
	return "0x" + strings.ToUpper(hex.EncodeToString(b))
}

func sqlLiteral(v interface{}) string {
	switch value := v.(type) {
	case nil:
		return "NULL"
	case bool:
		if value {
			return "1"
		}
		return "0"
	case int:
		return fmt.Sprintf("%d", value)
	case int8:
		return fmt.Sprintf("%d", value)
	case int16:
		return fmt.Sprintf("%d", value)
	case int32:
		return fmt.Sprintf("%d", value)
	case int64:
		return fmt.Sprintf("%d", value)
	case uint:
		return fmt.Sprintf("%d", value)
	case uint8:
		return fmt.Sprintf("%d", value)
	case uint16:
		return fmt.Sprintf("%d", value)
	case uint32:
		return fmt.Sprintf("%d", value)
	case uint64:
		return fmt.Sprintf("%d", value)
	case float32:
		return fmt.Sprintf("%.9g", value)
	case float64:
		return fmt.Sprintf("%.17g", value)
	case string:
		return quoteString(value)
	case []byte:
		return hexLiteral(value)
	case time.Time:
		return quoteString(value.Format("2006-01-02 15:04:05.999999"))
	case decimal.Decimal:
		return value.String()
	case fmt.Stringer:
		return quoteString(value.String())
	default:
		return quoteString(fmt.Sprint(value))
	}
}

func quoteString(s string) string {
	if s == "" {
		return "''"
	}

	var parts []string
	var plain strings.Builder
	flushPlain := func() {
		if plain.Len() == 0 {
			return
		}
		parts = append(parts, "'"+strings.ReplaceAll(plain.String(), "'", "''")+"'")
		plain.Reset()
	}

	for i := 0; i < len(s); i++ {
		if s[i] == '\\' || s[i] < 0x20 || s[i] == 0x7f {
			flushPlain()
			parts = append(parts, fmt.Sprintf("CHAR(%d)", s[i]))
			continue
		}
		plain.WriteByte(s[i])
	}
	flushPlain()

	if len(parts) == 1 {
		return parts[0]
	}
	return "CONCAT(" + strings.Join(parts, ", ") + ")"
}

func samplePending(pending map[string]struct{}, n int) string {
	keys := make([]string, 0, len(pending))
	for key := range pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > n {
		keys = keys[:n]
	}
	return strings.Join(keys, ", ")
}
