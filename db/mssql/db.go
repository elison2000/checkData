package mssql

import (
	"checkData/model"
	"checkData/util"
	"database/sql"
	"fmt"
	"github.com/gookit/slog"
)

type Database struct {
	SourceDb     string
	TargetDb     string
	SourceHost   string
	SourcePort   int
	TargetHost   string
	TargetPort   int
	SourceDbConn *sql.DB
	TargetDbConn *sql.DB
	Option       *model.Options
	Tables       *model.TableInfo
}

func (self *Database) getTables() (err error) {
	// 获取表名
	sql := `SELECT  TABLE_SCHEMA + '.' + TABLE_NAME as tb_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'`
	//获取源库所有表
	tableS, err := util.QueryReturnList(self.SourceDbConn, sql)
	if err != nil {
		return fmt.Errorf("getTables -> %w", err)
	}
	//保存
	for _, v := range tableS {
		self.Tables.Source = append(self.Tables.Source, v[0])
	}

	//获取目标库所有表
	tableT, err := util.QueryReturnList(self.TargetDbConn, sql)
	if err != nil {
		return fmt.Errorf("getTables -> %w", err)
	}
	//保存
	for _, v := range tableT {
		self.Tables.Target = append(self.Tables.Target, v[0])
	}

	return nil

}

func (self *Database) PreCheck() (err error) {
	//获取两端都存在的表

	if len(self.Tables.ToCheck) == 0 {
		err = self.getTables()
		if err != nil {
			return fmt.Errorf("GetToCheck-> %w", err)
		}

		//目标库不存在的表
		for _, t := range self.Tables.Source {
			if !util.InSlice(t, self.Tables.Target) {
				self.Tables.SourceMore = append(self.Tables.SourceMore, t)
			} else {
				self.Tables.ToCheck = append(self.Tables.ToCheck, t)
			}
		}

		//源库不存在的表
		for _, t := range self.Tables.Target {
			if !util.InSlice(t, self.Tables.Source) {
				self.Tables.TargetMore = append(self.Tables.TargetMore, t)
			}
		}

		//过滤不需要检查的表
		if len(self.Tables.Skip) > 0 {
			var tbs []string
			for _, tb := range self.Tables.ToCheck {
				if !util.InSlice(tb, self.Tables.Skip) {
					tbs = append(tbs, tb)
				}
			}
			self.Tables.ToCheck = tbs
		}
	}
	return
}

func (self *Database) GetTableInfo() *model.TableInfo {
	return self.Tables
}

func (self *Database) NewTable(tb string) model.Table {
	return &Table{
		DbName:      self.TargetDb,
		TbName:      tb,
		Mode:        self.Option.Mode,
		SkipColumns: self.Option.SkipColList,
		Keys:        self.Option.KeysList,
		Where:       self.Option.Where,
		DbGroup:     self,
		Result:      &model.Result{DbName: self.TargetDb, TbName: tb, RecheckPassRows: -1},
	}
}

func (self *Database) Close() {
	//关闭连接池
	self.SourceDbConn.Close()
	self.TargetDbConn.Close()
	slog.Infof("[%s:%s] 关闭数据库连接池", self.SourceDb, self.TargetDb)
}

func NewDatabase(opt *model.Options, dbg [2]string) (model.Database, error) {

	slog.Infof("[%s:%s] 开启数据库连接池", dbg[0], dbg[1])
	sdb, err := util.NewMssqlDB(opt.SourceHost, opt.SourcePort, opt.User, opt.Password, dbg[0])
	if err != nil {
		return nil, fmt.Errorf("NewDatabase -> %w", err)
	}
	tdb, err := util.NewMssqlDB(opt.TargetHost, opt.TargetPort, opt.TargetUser, opt.TargetPassword, dbg[1])
	if err != nil {
		return nil, fmt.Errorf("NewDatabase -> %w", err)
	}

	db := Database{
		SourceDb:     dbg[0],
		TargetDb:     dbg[1],
		SourceHost:   opt.SourceHost,
		TargetHost:   opt.TargetHost,
		SourcePort:   opt.SourcePort,
		TargetPort:   opt.TargetPort,
		SourceDbConn: sdb,
		TargetDbConn: tdb,
		Option:       opt,
		Tables:       &model.TableInfo{},
	}

	db.Tables.ToCheck = opt.TableList
	db.Tables.Skip = opt.SkipTableList

	var i model.Database = &db
	return i, nil
}
