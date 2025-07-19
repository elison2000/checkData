package model

import (
    "fmt"
    "os"
    "strconv"
    "strings"
)

type Options struct {
    Source          string
    Target          string
    User            string
    Password        string
    TargetUser      string
    TargetPassword  string
    DbType          string
    Mode            string
    Db              string
    Tables          string
    SkipCols        string
    SkipTables      string
    SourceHost      string
    SourcePort      int
    TargetHost      string
    TargetPort      int
    Where           string
    Keys            string
    DbList          []string
    DbGroupList     [][2]string
    TableList       []string
    SkipColList     []string
    SkipTableList   []string
    KeysList        []string
    Parallel        int
    MaxRecheckTimes int
    MaxRecheckRows  int
    Capacity        int //不一致的行数超过这个数，直接退出
    BaseDir         string
}

func (self *Options) Init() {

    //处理source参数
    s := strings.Split(self.Source, ":")
    if len(s) == 2 {
        self.SourceHost = s[0]
        port, _ := strconv.ParseUint(s[1], 10, 64)
        self.SourcePort = int(port)
    }

    //处理target参数
    t := strings.Split(self.Target, ":")
    if len(t) == 2 {
        self.TargetHost = t[0]
        port, _ := strconv.ParseUint(t[1], 10, 64)
        self.TargetPort = int(port)
    }
    if self.SourcePort == 0 || self.TargetPort == 0 {
        fmt.Println("source或target参数无效")
        os.Exit(1)
    }

    self.BaseDir = fmt.Sprintf("%s_%d", self.TargetHost, self.TargetPort)

    //用户账号
    if self.User == "" {
        fmt.Println("用户名不能为空")
        os.Exit(1)
    }

    if self.TargetUser == "" {
        self.TargetUser = self.User
    }

    if self.TargetPassword == "" {
        self.TargetPassword = self.Password
    }

    //处理db参数
    if self.Db != "" {
        self.DbList = strings.Split(self.Db, ",")
    } else {
        fmt.Println("db参数无效:", self.Db)
        os.Exit(1)
    }

    if len(self.DbList) == 0 {
        fmt.Println("db参数无效:", self.Db)
        os.Exit(1)
    }
    for _, dbstr := range self.DbList {
        var dbgroup [2]string
        g := strings.Split(dbstr, ":")
        if len(g) == 2 {
            dbgroup[0] = g[0]
            dbgroup[1] = g[1]
        } else {
            dbgroup[0] = g[0]
            dbgroup[1] = g[0]
        }
        self.DbGroupList = append(self.DbGroupList, dbgroup)
    }

    //处理tables参数
    if self.Tables != "" {
        self.TableList = strings.Split(self.Tables, ",")
    }

    //处理SkipCols参数
    if self.SkipCols != "" {
        self.SkipColList = strings.Split(self.SkipCols, ",")
    }

    //处理SkipTables参数
    if self.SkipTables != "" {
        self.SkipTableList = strings.Split(self.SkipTables, ",")
    }

    //keys
    if self.Keys != "" {
        self.KeysList = strings.Split(self.Keys, ",")
    }

    //并行线程数
    if self.Parallel == 0 {
        self.Parallel = 1
    }

    //容量
    if self.Capacity == 0 {
        self.Capacity = 10000
    }

}
