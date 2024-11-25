package mysql

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
    SourceDbConn *util.MysqlDb
    TargetDbConn *util.MysqlDb
    Tables       *model.Tables
}

func NewDatabase(opt *model.Options, dbg [2]string) (*Database, error) {

    db := Database{
        SourceDb:     dbg[0],
        TargetDb:     dbg[1],
        SourceHost:   opt.SourceHost,
        TargetHost:   opt.TargetHost,
        SourcePort:   opt.SourcePort,
        TargetPort:   opt.TargetPort,
        SourceDbConn: &util.MysqlDb{Host: opt.SourceHost, Port: opt.SourcePort, User: opt.User, Password: opt.Password, Database: dbg[0]},
        TargetDbConn: &util.MysqlDb{Host: opt.TargetHost, Port: opt.TargetPort, User: opt.TargetUser, Password: opt.TargetPassword, Database: dbg[1]},
        Tables:       &model.Tables{},
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

    return &db, nil
}

func (self *Database) getTables() (err error) {
    // 获取表名
    sql := fmt.Sprintf("show tables")
    //获取源库所有表
    tableS, err := self.SourceDbConn.QueryReturnList(sql)
    if err != nil {
        return fmt.Errorf("getTables -> %w", err)
    }
    //保存
    for _, v := range tableS {
        self.Tables.Source = append(self.Tables.Source, v[0])
    }

    //获取目标库所有表
    tableT, err := self.TargetDbConn.QueryReturnList(sql)
    if err != nil {
        return fmt.Errorf("getTables -> %w", err)
    }
    //保存
    for _, v := range tableT {
        self.Tables.Target = append(self.Tables.Target, v[0])
    }

    return nil

}

func (self *Database) GetToCheck() (err error) {
    //获取两端都存在的表

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

func (self *Database) Close() {
    //关闭连接池
    self.SourceDbConn.Close()
    self.TargetDbConn.Close()
    slog.Infof("[%s:%s] 关闭数据库连接池", self.SourceDb, self.TargetDb)
}
