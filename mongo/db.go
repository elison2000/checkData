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
    SourceDbConn *util.MongoDb
    TargetDbConn *util.MongoDb
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
        SourceDbConn: &util.MongoDb{Host: opt.SourceHost, Port: opt.SourcePort, User: opt.User, Password: opt.Password, Database: dbg[0]},
        TargetDbConn: &util.MongoDb{Host: opt.TargetHost, Port: opt.TargetPort, User: opt.TargetUser, Password: opt.TargetPassword, Database: dbg[1]},
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

func (self *Database) GetToCheck() (err error) {
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
