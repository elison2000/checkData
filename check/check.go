package check

import (
	"checkData/mongo"
	"checkData/mysql"
	"checkData/pgsql"
	"checkData/util"
	"fmt"
	"github.com/gookit/slog"
	"strings"
	"sync"
)

func CreateParallelPool(parallel int, todoQue, doneQue chan Checker) {
	/* 并行池使用方法
	在主函数中执行 go CreateParallelPool(parallel,todoQue, doneQue)
	执行后，往todoQue推送已实现Checker接口的对象，然后关闭close(todoQue)，最后等待取出doneQue数据 */

	defer close(doneQue)
	defer slog.Infof("关闭并行池，parallel=%d", parallel)
	slog.Infof("开启并行池，parallel=%d", parallel)
	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, todoQue, doneQue chan Checker) {
			defer wg.Done()
			for todo := range todoQue {
				CheckTable(todo) //这里的todo必须是指针类型*MysqlTable
				doneQue <- todo
			}
		}(&wg, todoQue, doneQue)
	}
	//等待所有任务执行完毕
	wg.Wait()
}

type Checker interface {
	Init()                         //初始化
	Precheck() bool                //预检查
	GetMode() string               //模式
	CheckTableCount()              //核对总记录数
	CheckTableDetail()             //核对明细数据
	GetResults() util.CheckResults //返回核对结果
}

func CheckTable(intf Checker) {
	//核对表入口

	// 初始化
	intf.Init()

	//预检查
	if intf.Precheck() {
		//核对
		if intf.GetMode() == "count" {
			intf.CheckTableCount()
		} else {
			intf.CheckTableDetail()
		}
	}

}

func CheckMysqlDB(opt util.Options, dbgroup [2]string) {
	//核对mysql数据库
	defer util.TimeCost()(fmt.Sprintf(
		"[%s:%s] 数据库核对完成", dbgroup[0], dbgroup[1]))

	yesTables := []util.CheckResults{}
	noTables := []util.CheckResults{}
	unknownTables := []util.CheckResults{}
	noTableNames := []string{}
	unknownTableNames := []string{}

	//创建数据库对象
	db := &mysql.MysqlDatabase{}
	db.Init(opt, dbgroup)
	//获取需要核对的表名
	db.GetToCheckTables()
	defer db.Close()

	//开始核对
	slog.Infof("[%s:%s] 开始核对数据库 [SOURCE端表数:%d  TARGET端表数:%d  两端共有表数:%d]", dbgroup[0], dbgroup[1], len(db.SourceTables), len(db.TargetTables), len(db.ToCheckTables))

	//1、开启一个并行池，然后把任务放入并行池，最后取出结果
	todoQue := make(chan Checker, len(db.ToCheckTables))
	doneQue := make(chan Checker, len(db.ToCheckTables))
	go CreateParallelPool(opt.Parallel, todoQue, doneQue)

	//2、把任务放入并行池
	//var todo *Checker
	for _, tbname := range db.ToCheckTables {
		todo := mysql.NewMysqlTable(tbname, db, opt)
		todoQue <- todo
	}

	//3、关闭队列
	close(todoQue)

	//4、取出结果
	fieldnames := "TableName, Result, ExecuteSeconds, SourceRows, TargetRows, SameRows, DiffRows, SourceMoreRows, TargetMoreRows, RecheckPassRows, Error\n"
	util.WriteFile(db.ReportFile, fieldnames)
	for doneTask := range doneQue {
		res := doneTask.GetResults()
		text := fmt.Sprintf("%s, %s, %d,%d, %d, %d, %d, %d, %d, %d, %s\n", res.TbName, res.Result, res.ExecuteSeconds, res.SourceRows, res.TargetRows, res.SameRows, res.DiffRows, res.SourceMoreRows, res.TargetMoreRows, res.RecheckPassRows, res.Error)
		util.WriteFileTail(db.ReportFile, text)
		//根据核对结果存入3个列表
		switch res.Result {
		case "yes":
			yesTables = append(yesTables, res)
		case "no":
			noTables = append(noTables, res)
			noTableNames = append(noTableNames, res.TbName)
		default:
			unknownTables = append(unknownTables, res)
			unknownTableNames = append(unknownTableNames, res.TbName)
		}

	}

	slog.Infof("[%s:%s] 核对报告：%s", dbgroup[0], dbgroup[1], db.ReportFile)
	//打印核对报告
	text := "########################################## 核对报告 ################################################\n"
	text += fmt.Sprintf("计划核对的数据库 : %s:%s\n", dbgroup[0], dbgroup[1])
	text += fmt.Sprintf("SOURCE端的表数   : %d\n", len(db.SourceTables))
	text += fmt.Sprintf("TARGET端的表数   : %d\n", len(db.TargetTables))
	text += fmt.Sprintf("两端共有的表数   : %d\n", len(db.ToCheckTables))
	text += fmt.Sprintf("数据一致的表数   : %d\n", len(yesTables))
	text += fmt.Sprintf("数据不一致的表数 : %d\n", len(noTables))
	text += fmt.Sprintf("核对失败的表数   : %d\n", len(unknownTables))
	text += fmt.Sprintf("SOURCE端缺失的表 : %s\n", strings.Join(db.TargetMoreTables, ", "))
	text += fmt.Sprintf("TARGET端缺失的表 : %s\n", strings.Join(db.SourceMoreTables, ", "))
	text += fmt.Sprintf("核对失败的表     : %s\n", strings.Join(unknownTableNames, ", "))
	text += "####################################################################################################\n"
	text += "核对结果说明\n"
	text += "Result          : yes-数据一致，no-数据不一致，unknown-未知（核对失败）\n"
	text += "ExecuteSeconds  : 执行时间，包括复核的时间（秒）\n"
	text += "SourceRows      : 源表总行数\n"
	text += "TargetRows      : 目标表总行数\n"
	text += "SameRows        : 数据一致的行数\n"
	text += fmt.Sprintf("DiffRows        : 数据不一致的行数，相关数据的主键保存在: ./%s/$table.diff\n", db.Logdir)
	text += fmt.Sprintf("SourceMoreRows  : 目标端缺失的数据行数，相关数据主键的保存在: ./%s/$table.tloss\n", db.Logdir)
	text += fmt.Sprintf("TargetMoreRows  : 目标端多出的数据行数，相关数据主键的保存在: ./%s/$table.tmore\n", db.Logdir)
	text += fmt.Sprintf("RecheckPassRows : 复核通过的行数，-1：表示没有进行复核\n")
	text += "####################################################################################################\n"
	text += fieldnames
	for _, res := range noTables {
		text += fmt.Sprintf("%s, %s, %d, %d, %d, %d, %d, %d, %d, %d, %s\n", res.TbName, res.Result, res.ExecuteSeconds, res.SourceRows, res.TargetRows, res.SameRows, res.DiffRows, res.SourceMoreRows, res.TargetMoreRows, res.RecheckPassRows, res.Error)
	}
	for _, res := range unknownTables {
		text += fmt.Sprintf("%s, %s, %d, %d, %d, %d, %d, %d, %d, %d, %s\n", res.TbName, res.Result, res.ExecuteSeconds, res.SourceRows, res.TargetRows, res.SameRows, res.DiffRows, res.SourceMoreRows, res.TargetMoreRows, res.RecheckPassRows, res.Error)
	}
	for _, res := range yesTables {
		text += fmt.Sprintf("%s, %s, %d, %d, %d, %d, %d, %d, %d, %d, %s\n", res.TbName, res.Result, res.ExecuteSeconds, res.SourceRows, res.TargetRows, res.SameRows, res.DiffRows, res.SourceMoreRows, res.TargetMoreRows, res.RecheckPassRows, res.Error)
	}

	//5、保存报告
	util.WriteFile(db.ReportFile, text)
}

func CheckMongoDB(opt util.Options, dbgroup [2]string) {
	//核对mongo数据库
	defer util.TimeCost()(fmt.Sprintf(
		"[%s:%s] 数据库核对完成", dbgroup[0], dbgroup[1]))

	yesTables := []util.CheckResults{}
	noTables := []util.CheckResults{}
	unknownTables := []util.CheckResults{}
	noTableNames := []string{}
	unknownTableNames := []string{}

	//创建数据库对象
	db := &mongo.MongoDatabase{}
	db.Init(opt, dbgroup)
	//获取需要核对的表名
	db.GetToCheckTables()
	defer db.Close()

	//开始核对
	slog.Infof("[%s:%s] 开始核对数据库 [SOURCE端表数:%d  TARGET端表数:%d  两端共有表数:%d]", dbgroup[0], dbgroup[1], len(db.SourceTables), len(db.TargetTables), len(db.ToCheckTables))

	//1、开启一个并行池，然后把任务放入并行池，最后取出结果
	todoQue := make(chan Checker, len(db.ToCheckTables))
	doneQue := make(chan Checker, len(db.ToCheckTables))
	go CreateParallelPool(opt.Parallel, todoQue, doneQue)

	//2、把任务放入并行池
	//var todo *Checker
	for _, tbname := range db.ToCheckTables {
		todo := mongo.NewMongoTable(tbname, db, opt)
		todoQue <- todo
	}

	//3、关闭队列
	close(todoQue)

	//4、取出结果
	fieldnames := "TableName, Result, ExecuteSeconds, SourceRows, TargetRows, SameRows, DiffRows, SourceMoreRows, TargetMoreRows, RecheckPassRows, Error\n"
	util.WriteFile(db.ReportFile, fieldnames)
	for doneTask := range doneQue {
		res := doneTask.GetResults()
		text := fmt.Sprintf("%s, %s, %d,%d, %d, %d, %d, %d, %d, %d, %s\n", res.TbName, res.Result, res.ExecuteSeconds, res.SourceRows, res.TargetRows, res.SameRows, res.DiffRows, res.SourceMoreRows, res.TargetMoreRows, res.RecheckPassRows, res.Error)
		util.WriteFileTail(db.ReportFile, text)
		//根据核对结果存入3个列表
		switch res.Result {
		case "yes":
			yesTables = append(yesTables, res)
		case "no":
			noTables = append(noTables, res)
			noTableNames = append(noTableNames, res.TbName)
		default:
			unknownTables = append(unknownTables, res)
			unknownTableNames = append(unknownTableNames, res.TbName)
		}

	}

	slog.Infof("[%s:%s] 核对报告：%s", dbgroup[0], dbgroup[1], db.ReportFile)
	//打印核对报告
	text := "########################################## 核对报告 ################################################\n"
	text += fmt.Sprintf("计划核对的数据库 : %s:%s\n", dbgroup[0], dbgroup[1])
	text += fmt.Sprintf("SOURCE端的表数   : %d\n", len(db.SourceTables))
	text += fmt.Sprintf("TARGET端的表数   : %d\n", len(db.TargetTables))
	text += fmt.Sprintf("两端共有的表数   : %d\n", len(db.ToCheckTables))
	text += fmt.Sprintf("数据一致的表数   : %d\n", len(yesTables))
	text += fmt.Sprintf("数据不一致的表数 : %d\n", len(noTables))
	text += fmt.Sprintf("核对失败的表数   : %d\n", len(unknownTables))
	text += fmt.Sprintf("SOURCE端缺失的表 : %s\n", strings.Join(db.TargetMoreTables, ", "))
	text += fmt.Sprintf("TARGET端缺失的表 : %s\n", strings.Join(db.SourceMoreTables, ", "))
	text += fmt.Sprintf("核对失败的表     : %s\n", strings.Join(unknownTableNames, ", "))
	text += "####################################################################################################\n"
	text += "核对结果说明\n"
	text += "Result          : yes-数据一致，no-数据不一致，unknown-未知（核对失败）\n"
	text += "ExecuteSeconds  : 执行时间，包括复核的时间（秒）\n"
	text += "SourceRows      : 源表总行数\n"
	text += "TargetRows      : 目标表总行数\n"
	text += "SameRows        : 数据一致的行数\n"
	text += fmt.Sprintf("DiffRows        : 数据不一致的行数，相关数据的主键保存在: ./%s/$table.diff\n", db.Logdir)
	text += fmt.Sprintf("SourceMoreRows  : 目标端缺失的数据行数，相关数据主键的保存在: ./%s/$table.tloss\n", db.Logdir)
	text += fmt.Sprintf("TargetMoreRows  : 目标端多出的数据行数，相关数据主键的保存在: ./%s/$table.tmore\n", db.Logdir)
	text += fmt.Sprintf("RecheckPassRows : 复核通过的行数，-1：表示没有进行复核\n")
	text += "####################################################################################################\n"
	text += fieldnames
	for _, res := range noTables {
		text += fmt.Sprintf("%s, %s, %d, %d, %d, %d, %d, %d, %d, %d, %s\n", res.TbName, res.Result, res.ExecuteSeconds, res.SourceRows, res.TargetRows, res.SameRows, res.DiffRows, res.SourceMoreRows, res.TargetMoreRows, res.RecheckPassRows, res.Error)
	}
	for _, res := range unknownTables {
		text += fmt.Sprintf("%s, %s, %d, %d, %d, %d, %d, %d, %d, %d, %s\n", res.TbName, res.Result, res.ExecuteSeconds, res.SourceRows, res.TargetRows, res.SameRows, res.DiffRows, res.SourceMoreRows, res.TargetMoreRows, res.RecheckPassRows, res.Error)
	}
	for _, res := range yesTables {
		text += fmt.Sprintf("%s, %s, %d, %d, %d, %d, %d, %d, %d, %d, %s\n", res.TbName, res.Result, res.ExecuteSeconds, res.SourceRows, res.TargetRows, res.SameRows, res.DiffRows, res.SourceMoreRows, res.TargetMoreRows, res.RecheckPassRows, res.Error)
	}

	//5、保存报告
	util.WriteFile(db.ReportFile, text)
}

func CheckPgsqlDB(opt util.Options, dbgroup [2]string) {
	//核对pgsql数据库
	defer util.TimeCost()(fmt.Sprintf(
		"[%s:%s] 数据库核对完成", dbgroup[0], dbgroup[1]))

	yesTables := []util.CheckResults{}
	noTables := []util.CheckResults{}
	unknownTables := []util.CheckResults{}
	noTableNames := []string{}
	unknownTableNames := []string{}

	//创建数据库对象
	db := &pgsql.PgsqlDatabase{}
	db.Init(opt, dbgroup)
	//获取需要核对的表名
	db.GetToCheckTables()
	defer db.Close()

	//开始核对
	slog.Infof("[%s:%s] 开始核对数据库 [SOURCE端表数:%d  TARGET端表数:%d  两端共有表数:%d]", dbgroup[0], dbgroup[1], len(db.SourceTables), len(db.TargetTables), len(db.ToCheckTables))

	//1、开启一个并行池，然后把任务放入并行池，最后取出结果
	todoQue := make(chan Checker, len(db.ToCheckTables))
	doneQue := make(chan Checker, len(db.ToCheckTables))
	go CreateParallelPool(opt.Parallel, todoQue, doneQue)

	//2、把任务放入并行池
	//var todo *Checker
	for _, tbname := range db.ToCheckTables {
		todo := pgsql.NewPgsqlTable(tbname, db, opt)
		todoQue <- todo
	}

	//3、关闭队列
	close(todoQue)

	//4、取出结果
	fieldnames := "TableName, Result, ExecuteSeconds, SourceRows, TargetRows, SameRows, DiffRows, SourceMoreRows, TargetMoreRows, RecheckPassRows, Error\n"
	util.WriteFile(db.ReportFile, fieldnames)
	for doneTask := range doneQue {
		res := doneTask.GetResults()
		text := fmt.Sprintf("%s, %s, %d,%d, %d, %d, %d, %d, %d, %s\n", res.TbName, res.Result, res.ExecuteSeconds, res.SourceRows, res.TargetRows, res.SameRows, res.DiffRows, res.SourceMoreRows, res.TargetMoreRows, res.Error)
		util.WriteFileTail(db.ReportFile, text)
		//根据核对结果存入3个列表
		switch res.Result {
		case "yes":
			yesTables = append(yesTables, res)
		case "no":
			noTables = append(noTables, res)
			noTableNames = append(noTableNames, res.TbName)
		default:
			unknownTables = append(unknownTables, res)
			unknownTableNames = append(unknownTableNames, res.TbName)
		}

	}

	slog.Infof("[%s:%s] 核对报告：%s", dbgroup[0], dbgroup[1], db.ReportFile)
	//打印核对报告
	text := "########################################## 核对报告 ################################################\n"
	text += fmt.Sprintf("计划核对的数据库 : %s:%s\n", dbgroup[0], dbgroup[1])
	text += fmt.Sprintf("SOURCE端的表数   : %d\n", len(db.SourceTables))
	text += fmt.Sprintf("TARGET端的表数   : %d\n", len(db.TargetTables))
	text += fmt.Sprintf("两端共有的表数   : %d\n", len(db.ToCheckTables))
	text += fmt.Sprintf("数据一致的表数   : %d\n", len(yesTables))
	text += fmt.Sprintf("数据不一致的表数 : %d\n", len(noTables))
	text += fmt.Sprintf("核对失败的表数   : %d\n", len(unknownTables))
	text += fmt.Sprintf("SOURCE端缺失的表 : %s\n", strings.Join(db.TargetMoreTables, ", "))
	text += fmt.Sprintf("TARGET端缺失的表 : %s\n", strings.Join(db.SourceMoreTables, ", "))
	text += fmt.Sprintf("核对失败的表     : %s\n", strings.Join(unknownTableNames, ", "))
	text += "####################################################################################################\n"
	text += "核对结果说明\n"
	text += "Result          : yes-数据一致，no-数据不一致，unknown-未知（核对失败）\n"
	text += "ExecuteSeconds  : 执行时间，包括复核的时间（秒）\n"
	text += "SourceRows      : 源表总行数\n"
	text += "TargetRows      : 目标表总行数\n"
	text += "SameRows        : 数据一致的行数\n"
	text += fmt.Sprintf("DiffRows        : 数据不一致的行数，相关数据的主键保存在: ./%s/$table.diff\n", db.Logdir)
	text += fmt.Sprintf("SourceMoreRows  : 目标端缺失的数据行数，相关数据主键的保存在: ./%s/$table.tloss\n", db.Logdir)
	text += fmt.Sprintf("TargetMoreRows  : 目标端多出的数据行数，相关数据主键的保存在: ./%s/$table.tmore\n", db.Logdir)
	text += fmt.Sprintf("RecheckPassRows : 复核通过的行数，-1：表示没有进行复核\n")
	text += "####################################################################################################\n"
	text += fieldnames
	for _, res := range noTables {
		text += fmt.Sprintf("%s, %s, %d, %d, %d, %d, %d, %d, %d, %d, %s\n", res.TbName, res.Result, res.ExecuteSeconds, res.SourceRows, res.TargetRows, res.SameRows, res.DiffRows, res.SourceMoreRows, res.TargetMoreRows, res.RecheckPassRows, res.Error)
	}
	for _, res := range unknownTables {
		text += fmt.Sprintf("%s, %s, %d, %d, %d, %d, %d, %d, %d, %d, %s\n", res.TbName, res.Result, res.ExecuteSeconds, res.SourceRows, res.TargetRows, res.SameRows, res.DiffRows, res.SourceMoreRows, res.TargetMoreRows, res.RecheckPassRows, res.Error)
	}
	for _, res := range yesTables {
		text += fmt.Sprintf("%s, %s, %d, %d, %d, %d, %d, %d, %d, %d, %s\n", res.TbName, res.Result, res.ExecuteSeconds, res.SourceRows, res.TargetRows, res.SameRows, res.DiffRows, res.SourceMoreRows, res.TargetMoreRows, res.RecheckPassRows, res.Error)
	}

	//5、保存报告
	util.WriteFile(db.ReportFile, text)
}
