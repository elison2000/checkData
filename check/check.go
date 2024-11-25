package check

import (
    "checkData/model"
    "checkData/mongo"
    "checkData/mysql"
    "checkData/pgsql"
    "checkData/threading"
    "checkData/util"
    "fmt"
    "github.com/gookit/slog"
    "os"
    "strings"
    "sync"
)

func StartToCheck(opt *model.Options) {
    util.EnterWorkDir()
    err := util.Mkdir(opt.BaseDir)
    if err != nil {
        slog.Error(err)
        os.Exit(1)
    }

    for _, group := range opt.DbGroupList {
        checkDB(opt, group)
    }
}
func checkDB(opt *model.Options, dbg [2]string) {
    dirName := fmt.Sprintf("%s/%s", opt.BaseDir, dbg[1])
    err := util.Mkdir(dirName)
    if err != nil {
        slog.Error(err)
        os.Exit(1)
    }

    //创建新的文件
    csvFile := fmt.Sprintf("%s/%s.csv", opt.BaseDir, dbg[1])
    fieldNames := "DbName,TableName,Status,ExecuteSeconds,SourceRows,TargetRows,SameRows,DiffRows,SourceMoreRows,TargetMoreRows,RecheckPassRows,Message\n"
    util.WriteFile(csvFile, fieldNames)

    //核对数据库
    var tables *model.Tables
    var results []*model.Result
    switch opt.DbType {
    case "mysql":
        tables, results = checkMysqlDB(opt, dbg)
    case "mongo":
        tables, results = checkMongoDB(opt, dbg)
    case "pgsql":
        tables, results = checkPgsqlDB(opt, dbg)
    case "default":
        slog.Errorf("不支持的数据库类型:%s", opt.DbType)
        return
    }

    //汇总结果
    var yesTables, noTables, unknownTables []string
    for _, v := range results {
        if v.Status == 1 {
            yesTables = append(yesTables, v.TbName)
        } else if v.Status == 0 {
            noTables = append(noTables, v.TbName)
        } else {
            unknownTables = append(unknownTables, v.TbName)
        }
    }

    reportFile := fmt.Sprintf("%s/%s.rpt", opt.BaseDir, dbg[1])
    slog.Infof("[%s:%s] 核对报告：%s", dbg[0], dbg[1], reportFile)

    var buf strings.Builder
    buf.WriteString("####################################################################################################\n")
    buf.WriteString("核对文件说明\n")
    buf.WriteString("rpt文件: 核对总览信息\n")
    buf.WriteString("csv文件: 核对明细信息\n")
    buf.WriteString("ExecuteSeconds  : 执行时间，包括复核的时间（秒）\n")
    buf.WriteString("SourceRows      : 源表总行数\n")
    buf.WriteString("TargetRows      : 目标表总行数\n")
    buf.WriteString("SameRows        : 数据一致的行数\n")
    buf.WriteString(fmt.Sprintf("DiffRows        : 数据不一致的行数，相关数据的主键保存在: ./%s/%s/$table.diff\n", opt.BaseDir, dbg[1]))
    buf.WriteString(fmt.Sprintf("SourceMoreRows  : 目标端缺失的数据行数，相关数据主键的保存在: ./%s/%s/$table.tloss\n", opt.BaseDir, dbg[1]))
    buf.WriteString(fmt.Sprintf("TargetMoreRows  : 目标端多出的数据行数，相关数据主键的保存在: ./%s/%s/$table.tmore\n", opt.BaseDir, dbg[1]))
    buf.WriteString(fmt.Sprintf("RecheckPassRows : 复核通过的行数，-1：表示没有进行复核\n"))
    buf.WriteString("########################################## 核对报告 ################################################\n")
    buf.WriteString(fmt.Sprintf("计划核对的数据库 : %s:%s\n", dbg[0], dbg[1]))
    buf.WriteString(fmt.Sprintf("SOURCE端的表数   : %d\n", len(tables.Source)))
    buf.WriteString(fmt.Sprintf("TARGET端的表数   : %d\n", len(tables.Target)))
    buf.WriteString(fmt.Sprintf("需要核对的表数   : %d\n", len(tables.ToCheck)))
    buf.WriteString(fmt.Sprintf("数据一致的表数   : %d\n", len(yesTables)))
    buf.WriteString(fmt.Sprintf("数据不一致的表数 : %d\n", len(noTables)))
    buf.WriteString(fmt.Sprintf("核对失败的表数   : %d\n", len(unknownTables)))
    buf.WriteString(fmt.Sprintf("SOURCE端缺失的表 : %s\n", strings.Join(tables.TargetMore, ", ")))
    buf.WriteString(fmt.Sprintf("TARGET端缺失的表 : %s\n", strings.Join(tables.SourceMore, ", ")))
    buf.WriteString(fmt.Sprintf("核对失败的表     : %s\n", strings.Join(unknownTables, ", ")))
    buf.WriteString("####################################################################################################\n")

    util.WriteFile(reportFile, buf.String())

}

func checkMysqlDB(opt *model.Options, dbg [2]string) (tbs *model.Tables, results []*model.Result) {

    defer util.TimeCost()(fmt.Sprintf("[%s:%s] 数据库核对完成", dbg[0], dbg[1]))
    slog.Infof("[%s:%s] 开始核对数据库", dbg[0], dbg[1])

    //创建数据库对象
    db, err := mysql.NewDatabase(opt, dbg)
    if err != nil {
        slog.Errorf("[%s:%s]  创建数据库连接报错：%s", dbg[0], dbg[1], err)
        return
    }
    defer db.Close()

    err = db.GetToCheck()
    if err != nil {
        slog.Errorf("[%s:%s]  获取要核对的表名报错：%s", dbg[0], dbg[1], err)
        return
    }
    tbs = db.Tables

    //开始核对
    slog.Infof("[%s:%s] 开始核对数据库 [SOURCE端表数:%d  TARGET端表数:%d  需要核对的表数:%d]", dbg[0], dbg[1], len(db.Tables.Source), len(db.Tables.Target), len(db.Tables.ToCheck))

    pool := threading.NewPool(opt.Parallel, 1000)
    pool.Start() //先执行Start，防止queue满导致堵塞

    mu := &sync.Mutex{}
    for _, tbName := range db.Tables.ToCheck {
        tb := mysql.NewTable(tbName, db, opt)
        chk := NewChecker(tb, opt)

        pool.AddTask(
            func() {
                chk.Start()
                mu.Lock()
                defer mu.Unlock()
                chk.SaveResult()
                results = append(results, chk.Result)
            })
    }

    //关闭队列
    pool.Close()

    //等待任务执行完成
    pool.Join()
    return
}

func checkMongoDB(opt *model.Options, dbg [2]string) (tbs *model.Tables, results []*model.Result) {

    defer util.TimeCost()(fmt.Sprintf("[%s:%s] 数据库核对完成", dbg[0], dbg[1]))
    slog.Infof("[%s:%s] 开始核对数据库", dbg[0], dbg[1])

    //创建数据库对象
    db, err := mongo.NewDatabase(opt, dbg)
    if err != nil {
        slog.Errorf("[%s:%s]  创建数据库连接报错：%s", dbg[0], dbg[1], err)
        return
    }
    defer db.Close()

    err = db.GetToCheck()
    if err != nil {
        slog.Errorf("[%s:%s]  获取要核对的表名报错：%s", dbg[0], dbg[1], err)
        return
    }
    tbs = db.Tables

    //开始核对
    slog.Infof("[%s:%s] 开始核对数据库 [SOURCE端表数:%d  TARGET端表数:%d  需要核对的表数:%d]", dbg[0], dbg[1], len(db.Tables.Source), len(db.Tables.Target), len(db.Tables.ToCheck))

    pool := threading.NewPool(opt.Parallel, 1000)
    pool.Start() //先执行Start，防止queue满导致堵塞

    mu := &sync.Mutex{}
    for _, tbName := range db.Tables.ToCheck {
        tb := mongo.NewTable(tbName, db, opt)
        chk := NewChecker(tb, opt)

        pool.AddTask(
            func() {
                chk.Start()
                mu.Lock()
                defer mu.Unlock()
                chk.SaveResult()
                results = append(results, chk.Result)
            })
    }

    //关闭队列
    pool.Close()

    //等待任务执行完成
    pool.Join()
    return
}

func checkPgsqlDB(opt *model.Options, dbg [2]string) (tbs *model.Tables, results []*model.Result) {

    defer util.TimeCost()(fmt.Sprintf("[%s:%s] 数据库核对完成", dbg[0], dbg[1]))
    slog.Infof("[%s:%s] 开始核对数据库", dbg[0], dbg[1])

    //创建数据库对象
    db, err := pgsql.NewDatabase(opt, dbg)
    if err != nil {
        slog.Errorf("[%s:%s]  创建数据库连接报错：%s", dbg[0], dbg[1], err)
        return
    }
    defer db.Close()

    err = db.GetToCheck()
    if err != nil {
        slog.Errorf("[%s:%s]  获取要核对的表名报错：%s", dbg[0], dbg[1], err)
        return
    }
    tbs = db.Tables

    //开始核对
    slog.Infof("[%s:%s] 开始核对数据库 [SOURCE端表数:%d  TARGET端表数:%d  需要核对的表数:%d]", dbg[0], dbg[1], len(db.Tables.Source), len(db.Tables.Target), len(db.Tables.ToCheck))

    pool := threading.NewPool(opt.Parallel, 1000)
    pool.Start() //先执行Start，防止queue满导致堵塞

    mu := &sync.Mutex{}
    for _, tbName := range db.Tables.ToCheck {
        tb := pgsql.NewTable(tbName, db, opt)
        chk := NewChecker(tb, opt)

        pool.AddTask(
            func() {
                chk.Start()
                mu.Lock()
                defer mu.Unlock()
                chk.SaveResult()
                results = append(results, chk.Result)
            })
    }

    //关闭队列
    pool.Close()

    //等待任务执行完成
    pool.Join()
    return
}
