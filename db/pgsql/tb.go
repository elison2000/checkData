package pgsql

import (
	"checkData/model"
	"checkData/util"
	"fmt"
	"github.com/gookit/slog"
	"os"
	"strings"
)

const quote = `"`

type Table struct {
	DbName         string
	TbName         string
	EnclosedTbName string
	Mode           string //fast,slow,count
	Keys           []string
	Columns        []string
	Where          string
	SkipColumns    []string
	KeysText       string
	ColumnsText    string
	SQLText        string
	DbGroup        *Database
	Result         *model.Result
}

func (self *Table) GetDbName() string {
	return self.DbName
}

func (self *Table) GetTbName() string {
	return self.TbName
}

func (self *Table) splitTableName() (string, string) {
	//拆分列名
	l := strings.Split(self.TbName, `.`)
	if len(l) != 2 {
		fmt.Printf("表名格式错误: %s (正确格式:schema_name.table_name)\n", self.TbName)
		os.Exit(-1)
	}
	schema := l[0]
	tb := l[1]
	return schema, tb
}

func (self *Table) getEnclosedTbName() {
	//self.EnclosedTbName = util.EncloseStr(self.TbName, quote)
	schema, tb := self.splitTableName()
	self.EnclosedTbName = util.EncloseStr(schema, quote) + "." + util.EncloseStr(tb, quote)
}

func (self *Table) getKeys() error {
	if len(self.Keys) > 0 {
		return nil
	}

	schema, tb := self.splitTableName()

	sql := fmt.Sprintf(`select pg_attribute.attname as column_name,pg_class.relname from pg_index, pg_class, pg_attribute, pg_namespace
where pg_namespace.oid = pg_class.relnamespace and pg_namespace.nspname = '%s' and pg_class.relname='%s' and indrelid = pg_class.oid and pg_attribute.attrelid = pg_class.oid and pg_attribute.attnum = any(pg_index.indkey) and indisprimary
order by array_position(pg_index.indkey, pg_attribute.attnum)`, schema, tb)

	rows, err := util.QueryReturnList(self.DbGroup.SourceDbConn, sql)
	if err != nil {
		return fmt.Errorf("getKeys -> %w", err)
	}

	for _, row := range rows {
		self.Keys = append(self.Keys, row[0])
	}

	slog.Infof("[%s.%s] 主键列: %s", self.DbName, self.TbName, strings.Join(self.Keys, ", "))
	return nil
}

func (self *Table) getColumns() error {
	// 获取列名
	schema, tb := self.splitTableName()
	sql := fmt.Sprintf(`select a.attname from pg_class c join pg_attribute a on a.attrelid = c.oid join pg_namespace n on n.oid = c.relnamespace
where a.attnum > 0 and n.nspname='%s' and c.relname = '%s' order by a.attnum`, schema, tb)
	rows, err := util.QueryReturnList(self.DbGroup.SourceDbConn, sql)
	if err != nil {
		return fmt.Errorf("getColumns -> %w", err)
	}

	for _, row := range rows {
		self.Columns = append(self.Columns, row[0])
	}

	return nil
}

func (self *Table) getCheckSQL() error {

	var sql string
	if self.Mode == "slow" {
		sql = fmt.Sprintf("select %s, %s from %s", self.KeysText, self.ColumnsText, self.EnclosedTbName)
	} else {
		sql = fmt.Sprintf("select concat_ws(',',%s) pk,crc32(concat_ws('|',%s)) chksum from %s", self.KeysText, self.ColumnsText, self.TbName)
	}

	if self.Where != "" {
		sql += " where " + self.Where
	}
	self.SQLText = sql + " order by " + self.KeysText

	return nil
}

func (self *Table) escapeValue(val string) string {
	// 此函数用于转义 值中的单引号和反斜杠等，生成修复SQL时需要使用
	// 值中的 ' -> ''
	// 值中的 \ -> \\  其他数据库不需要这个转义
	const (
		singleQuote = '\''
		backslash   = '\\'
	)
	buf := strings.Builder{}
	buf.Grow(len(val) + 1)
	for i := 0; i < len(val); i++ {
		b := val[i]
		if b == singleQuote {
			buf.WriteByte(b)
			buf.WriteByte(b)
		} else {
			buf.WriteByte(b)
		}
	}
	return buf.String()
}
