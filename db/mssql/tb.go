package mssql

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

	sql := fmt.Sprintf(`SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
where CONSTRAINT_NAME in (
SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND CONSTRAINT_TYPE = 'PRIMARY KEY'
)
order by ORDINAL_POSITION`, schema, tb)

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
	sql := fmt.Sprintf(`select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' order by ORDINAL_POSITION`, schema, tb)

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
		sql = fmt.Sprintf("select %s as pk,crc32(concat(%s)) as rowdata from %s", self.KeysText, self.ColumnsText, self.TbName)
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
		if b == singleQuote || b == backslash {
			buf.WriteByte(b)
			buf.WriteByte(b)
		} else {
			buf.WriteByte(b)
		}
	}
	return buf.String()
}
