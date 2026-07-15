package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/parser"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/shopspring/decimal"
)

func mustParseGTIDSet(t *testing.T, raw string) *gomysql.MysqlGTIDSet {
	t.Helper()
	set, err := parseMysqlGTIDSet(raw)
	if err != nil {
		t.Fatalf("parse GTID set: %v", err)
	}
	return set
}

func TestSubtractGTIDSets(t *testing.T) {
	left := mustParseGTIDSet(t, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-5,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:10")
	right := mustParseGTIDSet(t, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-3")

	got := subtractGTIDSets(left, right).String()
	want := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:4-5,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:10"
	if got != want {
		t.Fatalf("unexpected diff\nwant: %s\n got: %s", want, got)
	}
}

func TestEnumerateGTIDs(t *testing.T) {
	set := mustParseGTIDSet(t, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:4-5")

	got, err := enumerateGTIDs(set, 10)
	if err != nil {
		t.Fatalf("enumerate GTIDs: %v", err)
	}
	for _, gtid := range []string{
		"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:4",
		"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:5",
	} {
		if _, ok := got[gtid]; !ok {
			t.Fatalf("missing %s in %#v", gtid, got)
		}
	}
}

func TestReplicationEndpoint(t *testing.T) {
	cfg, err := mysql.ParseDSN("repl:pwd@tcp(10.1.2.3:3307)/")
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := replicationEndpoint(cfg)
	if err != nil {
		t.Fatalf("replication endpoint: %v", err)
	}
	if host != "10.1.2.3" || port != 3307 {
		t.Fatalf("unexpected endpoint %s:%d", host, port)
	}
}

func TestLoadFileConfigAndBuildMySQLConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	err := os.WriteFile(path, []byte(`{
  "mysql": {
    "user": "repl",
    "password": "base-secret",
    "port": 3306,
    "params": {
      "timeout": "5s"
    }
  },
  "old_master": {
    "password": "old-secret",
    "port": 3307
  }
}`), 0600)
	if err != nil {
		t.Fatal(err)
	}

	fileCfg, err := loadFileConfig(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	newCfg, err := buildMySQLConfig("10.0.0.11", fileCfg.MySQL, fileCfg.NewMaster)
	if err != nil {
		t.Fatalf("build new config: %v", err)
	}
	if newCfg.User != "repl" || newCfg.Passwd != "base-secret" || newCfg.Addr != "10.0.0.11:3306" {
		t.Fatalf("unexpected new config: %#v", newCfg)
	}
	if newCfg.Params["charset"] != "utf8mb4" || newCfg.Params["timeout"] != "5s" {
		t.Fatalf("unexpected new params: %#v", newCfg.Params)
	}

	oldCfg, err := buildMySQLConfig("10.0.0.12", fileCfg.MySQL, fileCfg.OldMaster)
	if err != nil {
		t.Fatalf("build old config: %v", err)
	}
	if oldCfg.User != "repl" || oldCfg.Passwd != "old-secret" || oldCfg.Addr != "10.0.0.12:3307" {
		t.Fatalf("unexpected old config: %#v", oldCfg)
	}
}

func TestBuildMySQLConfigRejectsPositionalPort(t *testing.T) {
	_, err := buildMySQLConfig("10.0.0.11:3306", mysqlAuthConfig{User: "repl", Port: 3306}, mysqlAuthConfig{})
	if err == nil {
		t.Fatal("expected an error for host:port positional argument")
	}
}

func TestBuildMySQLConfigAllowsIPv6AddressWithoutPort(t *testing.T) {
	cfg, err := buildMySQLConfig("2001:db8::1", mysqlAuthConfig{User: "repl", Port: 3306}, mysqlAuthConfig{})
	if err != nil {
		t.Fatalf("build IPv6 config: %v", err)
	}
	if cfg.Addr != "[2001:db8::1]:3306" {
		t.Fatalf("unexpected IPv6 addr: %s", cfg.Addr)
	}
}

func TestWriteInsertSkipsMissingColumns(t *testing.T) {
	var out bytes.Buffer
	a := &app{out: &out}
	err := a.writeInsert(
		tableName{Schema: "db", Table: "t"},
		tableSchema{Columns: testColumns("id", "name", "note")},
		[]interface{}{int64(1), "O'Reilly", nil},
		map[int]struct{}{2: {}},
	)
	if err != nil {
		t.Fatalf("write insert: %v", err)
	}

	sql := out.String()
	if !strings.Contains(sql, "INSERT INTO `db`.`t` (`id`, `name`) VALUES (1, 'O''Reilly');") {
		t.Fatalf("unexpected SQL: %s", sql)
	}
	assertMySQLCompatibleSyntax(t, sql)
}

func TestWriteDeleteUsesBeforeImage(t *testing.T) {
	var out bytes.Buffer
	a := &app{out: &out}
	err := a.writeDelete(
		tableName{Schema: "db", Table: "t"},
		tableSchema{Columns: []columnSchema{
			{Name: "id", DataType: "bigint"},
			{Name: "name", DataType: "varchar", Text: true},
			{Name: "deleted_at", DataType: "datetime"},
		}},
		[]interface{}{int64(1), "O'Reilly", nil},
		nil,
	)
	if err != nil {
		t.Fatalf("write delete: %v", err)
	}

	sql := out.String()
	want := "DELETE FROM `db`.`t` WHERE `id` = 1 AND `name` = 'O''Reilly' AND `deleted_at` IS NULL LIMIT 1;"
	if !strings.Contains(sql, want) {
		t.Fatalf("unexpected SQL\nwant fragment: %s\n          got: %s", want, sql)
	}
	assertMySQLCompatibleSyntax(t, sql)
}

func TestWriteUpdateUsesBeforeWhereAndChangedAfterSet(t *testing.T) {
	var out bytes.Buffer
	a := &app{out: &out}
	err := a.writeUpdate(
		tableName{Schema: "db", Table: "t"},
		tableSchema{Columns: []columnSchema{
			{Name: "id", DataType: "bigint"},
			{Name: "name", DataType: "varchar", Text: true},
			{Name: "note", DataType: "varchar", Text: true},
		}},
		[]interface{}{int64(1), "old", "same"},
		[]interface{}{int64(1), "new", "same"},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("write update: %v", err)
	}

	sql := out.String()
	want := "UPDATE `db`.`t` SET `name` = 'new' WHERE `id` = 1 AND `name` = 'old' AND `note` = 'same' LIMIT 1;"
	if !strings.Contains(sql, want) {
		t.Fatalf("unexpected SQL\nwant fragment: %s\n          got: %s", want, sql)
	}
	assertMySQLCompatibleSyntax(t, sql)
}

func TestWriteUpdateSupportsPartialRowImages(t *testing.T) {
	var out bytes.Buffer
	a := &app{out: &out}
	err := a.writeUpdate(
		tableName{Schema: "db", Table: "t"},
		tableSchema{Columns: []columnSchema{
			{Name: "id", DataType: "bigint"},
			{Name: "name", DataType: "varchar", Text: true},
			{Name: "note", DataType: "varchar", Text: true},
		}},
		[]interface{}{int64(1), nil, nil},
		[]interface{}{nil, "new", nil},
		map[int]struct{}{1: {}, 2: {}},
		map[int]struct{}{0: {}, 2: {}},
	)
	if err != nil {
		t.Fatalf("write update: %v", err)
	}

	sql := out.String()
	want := "UPDATE `db`.`t` SET `name` = 'new' WHERE `id` = 1 LIMIT 1;"
	if !strings.Contains(sql, want) {
		t.Fatalf("unexpected SQL\nwant fragment: %s\n          got: %s", want, sql)
	}
	assertMySQLCompatibleSyntax(t, sql)
}

func TestQuoteStringIsSafeForMySQL57And80SQLModes(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty",
			in:   "",
			want: "''",
		},
		{
			name: "single quote",
			in:   "O'Reilly",
			want: "'O''Reilly'",
		},
		{
			name: "backslash",
			in:   `C:\tmp\file`,
			want: "CONCAT('C:', CHAR(92), 'tmp', CHAR(92), 'file')",
		},
		{
			name: "control bytes",
			in:   "a\nb\r\x00\x1ac",
			want: "CONCAT('a', CHAR(10), 'b', CHAR(13), CHAR(0), CHAR(26), 'c')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := quoteString(tt.in)
			if got != tt.want {
				t.Fatalf("unexpected quoteString result\nwant: %s\n got: %s", tt.want, got)
			}
			assertMySQLCompatibleSyntax(t, "INSERT INTO `t` (`c`) VALUES ("+got+");")
		})
	}
}

func TestSQLLiteralBytesUseMySQLHexLiteral(t *testing.T) {
	got := sqlLiteral([]byte{0x00, 0x41, 0xab, 0xff})
	want := "0x0041ABFF"
	if got != want {
		t.Fatalf("unexpected byte literal\nwant: %s\n got: %s", want, got)
	}
	assertMySQLCompatibleSyntax(t, "INSERT INTO `t` (`b`) VALUES ("+got+");")
}

func TestWriteInsertUsesMySQL57And80CompatibleSyntax(t *testing.T) {
	var out bytes.Buffer
	a := &app{out: &out}
	err := a.writeInsert(
		tableName{Schema: "select", Table: "rank"},
		tableSchema{Columns: []columnSchema{
			{Name: "window", DataType: "bigint"},
			{Name: "groups", DataType: "varchar", Text: true},
			{Name: "order", DataType: "varchar", Text: true},
			{Name: "json", DataType: "json", Text: true},
			{Name: "binary", DataType: "varbinary", Binary: true},
			{Name: "created at", DataType: "datetime"},
		}},
		[]interface{}{
			int64(42),
			`C:\tmp\file`,
			"line\nbreak",
			`{"k":"v"}`,
			[]byte{0x00, 0xab, 0xff},
			time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("write insert: %v", err)
	}

	sql := out.String()
	wantFragments := []string{
		"INSERT INTO `select`.`rank`",
		"`window`, `groups`, `order`, `json`, `binary`, `created at`",
		"CONCAT('C:', CHAR(92), 'tmp', CHAR(92), 'file')",
		"CONCAT('line', CHAR(10), 'break')",
		"'{\"k\":\"v\"}'",
		"0x00ABFF",
		"'2024-01-02 03:04:05.123456'",
	}
	for _, fragment := range wantFragments {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("SQL missing %q: %s", fragment, sql)
		}
	}
	assertMySQLCompatibleSyntax(t, sql)
}

func TestWriteInsertMatchesMySQL8DatatypeValues(t *testing.T) {
	var out bytes.Buffer
	a := &app{out: &out}
	err := a.writeInsert(
		tableName{Schema: "db", Table: "all_mysql8_datatype_demo"},
		tableSchema{Columns: []columnSchema{
			buildColumnSchema("col_tinyint", "tinyint", "tinyint", ""),
			buildColumnSchema("col_tinyint_u", "tinyint", "tinyint unsigned", ""),
			buildColumnSchema("col_smallint_u", "smallint", "smallint unsigned", ""),
			buildColumnSchema("col_mediumint_u", "mediumint", "mediumint unsigned", ""),
			buildColumnSchema("col_int_u", "int", "int unsigned", ""),
			buildColumnSchema("col_decimal", "decimal", "decimal(18,6)", ""),
			buildColumnSchema("col_binary", "binary", "binary(8)", ""),
			buildColumnSchema("col_varbinary", "varbinary", "varbinary(255)", ""),
			buildColumnSchema("col_text", "text", "text", "utf8mb4"),
			buildColumnSchema("col_blob", "blob", "blob", ""),
			buildColumnSchema("col_enum", "enum", "enum('male','female','unknown')", "utf8mb4"),
			buildColumnSchema("col_set", "set", "set('read','write','delete','admin')", "utf8mb4"),
		}},
		[]interface{}{
			int8(-100),
			int8(-56),
			int16(-5536),
			int32(-777216),
			int32(-294967296),
			decimal.RequireFromString("999999999999.123456"),
			"hello123",
			"testbin",
			[]byte("普通text大段文字，支持中文、符号！@#￥%……&*"),
			[]byte{0x11, 0x22, 0x33, 0x44, 0x55},
			int64(1),
			int64(9),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("write insert: %v", err)
	}

	sql := out.String()
	wantFragments := []string{
		"`col_tinyint`, `col_tinyint_u`, `col_smallint_u`, `col_mediumint_u`, `col_int_u`",
		"VALUES (-100, 200, 60000, 16000000, 4000000000",
		"999999999999.123456",
		"0x68656C6C6F313233",
		"0x7465737462696E",
		"'普通text大段文字，支持中文、符号！@#￥%……&*'",
		"0x1122334455",
		"'male'",
		"'read,admin'",
	}
	for _, fragment := range wantFragments {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("SQL missing %q: %s", fragment, sql)
		}
	}
	assertMySQLCompatibleSyntax(t, sql)
}

func TestWriteHeaderUsesMySQL57And80CompatibleStatements(t *testing.T) {
	var out bytes.Buffer
	a := &app{
		out:    &out,
		errant: mustParseGTIDSet(t, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:4"),
	}
	err := a.writeHeader(
		mustParseGTIDSet(t, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-3"),
		mustParseGTIDSet(t, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-4"),
	)
	if err != nil {
		t.Fatalf("write header: %v", err)
	}

	if !strings.Contains(out.String(), "SET NAMES utf8mb4;") {
		t.Fatalf("header did not include SET NAMES utf8mb4: %s", out.String())
	}
	assertMySQLCompatibleSyntax(t, "SET NAMES utf8mb4;")
}

func assertMySQLCompatibleSyntax(t *testing.T, sql string) {
	t.Helper()

	p := parser.New()
	if _, _, err := p.Parse(sql, "", ""); err != nil {
		t.Fatalf("generated SQL is not valid MySQL syntax: %v\nSQL: %s", err, sql)
	}
}

func testColumns(names ...string) []columnSchema {
	columns := make([]columnSchema, 0, len(names))
	for _, name := range names {
		columns = append(columns, columnSchema{Name: name, DataType: "varchar", Text: true})
	}
	return columns
}
