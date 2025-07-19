package mongo

import (
	"checkData/model"
	"checkData/util"
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
	SourceDbConn *util.MongoDB
	TargetDbConn *util.MongoDB
	Option       *model.Options
	Tables       *model.TableInfo
}

func (self *Database) getTables() (err error) {
	// 获取表名

	//获取源库所有表
	res1, err := self.SourceDbConn.ListCollectionNames(self.SourceDb)
	if err != nil {
		slog.Errorf("[%s:%s] 获取表名失败，%s", self.SourceDb, self.TargetDb, err)
		return
	}

	//剔除系统表
	var filtRes1 []string
	for _, v := range res1 {
		if v != "system.profile" {
			filtRes1 = append(filtRes1, v)
		}
	}
	self.Tables.Source = filtRes1

	//获取目标库所有表
	res2, err := self.TargetDbConn.ListCollectionNames(self.TargetDb)
	if err != nil {
		slog.Errorf("[%s:%s] 获取表名失败，%s", self.SourceDb, self.TargetDb, err)
	}

	//剔除系统表
	var filtRes2 []string
	for _, v := range res2 {
		if v != "system.profile" {
			filtRes2 = append(filtRes2, v)
		}
	}
	self.Tables.Target = filtRes1
	return nil

}

func (self *Database) PreCheck() (err error) {
	if len(self.Tables.ToCheck) == 0 {
		err = self.getTables()
		if err != nil {
			return fmt.Errorf("GetToCheck -> %w", err)
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
	return nil
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

	db := Database{
		SourceDb:     dbg[0],
		TargetDb:     dbg[1],
		SourceHost:   opt.SourceHost,
		TargetHost:   opt.TargetHost,
		SourcePort:   opt.SourcePort,
		TargetPort:   opt.TargetPort,
		SourceDbConn: &util.MongoDB{Host: opt.SourceHost, Port: opt.SourcePort, User: opt.User, Password: opt.Password, Database: dbg[0]},
		TargetDbConn: &util.MongoDB{Host: opt.TargetHost, Port: opt.TargetPort, User: opt.TargetUser, Password: opt.TargetPassword, Database: dbg[1]},
		Option:       opt,
		Tables:       &model.TableInfo{},
	}

	slog.Infof("[%s:%s] 开启数据库连接池", db.SourceDb, db.TargetDb)
	err := db.SourceDbConn.Init()
	if err != nil {
		return nil, fmt.Errorf("NewDatabase -> %w", err)
	}
	err = db.TargetDbConn.Init()
	if err != nil {
		return nil, fmt.Errorf("NewDatabase -> %w", err)
	}

	db.Tables.ToCheck = opt.TableList
	db.Tables.Skip = opt.SkipTableList

	var i model.Database = &db
	return i, nil

}
