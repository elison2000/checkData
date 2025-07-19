package main

import (
	"checkData/check"
	"checkData/model"
	"fmt"
	"github.com/gookit/slog"
	"github.com/urfave/cli/v2"
	"log"
	//_ "net/http/pprof"
	"os"
)

func version() {
	text := `
####################################################################################################
#  Name        :  checkData
#  Author      :  Elison
#  Email       :  Ly99@qq.com
#  Description :  核对迁移或同步的数据库两端的数据是否一致,支持mysql(tidb/doris)、pgsql、mongo等多种数据库
#  Updates     :
#      Version     When            What
#      --------    -----------     -----------------------------------------------------------------
#      v2.0        2023-01-20      优化性能，使用golang替换python
#      v2.1.0      2023-02-10      为了兼容其他数据库，使用interface重构Checker逻辑
#      v2.1.1      2023-02-12      增加mongo核对功能
#      v2.1.2      2023-02-13      增加pgsql核对功能
#      v2.1.3      2024-11-23      优化checker逻辑，修复bug
#      v2.1.4      2025-01-14      修复count模式下检查主键bug
#      v2.1.5      2025-01-27      增加sql server核对功能（用到concat函数，需要2012以上版本）
#      v2.1.6      2025-03-10      添加oceanbase
#      v2.1.7      2025-07-19      修复bug:复核逻辑和导数逻辑
####################################################################################################
`
	fmt.Println(text)
}

func GetOptions(ctx *cli.Context) *model.Options {
	opt := model.Options{}
	opt.Source = ctx.String("source")
	opt.Target = ctx.String("target")
	opt.User = ctx.String("user")
	opt.Password = ctx.String("password")
	opt.TargetUser = ctx.String("target-user")
	opt.TargetPassword = ctx.String("target-password")
	opt.Mode = ctx.String("mode")
	opt.Db = ctx.String("db")
	opt.Tables = ctx.String("tables")
	opt.Where = ctx.String("where")
	opt.SkipCols = ctx.String("skipcols")
	opt.SkipTables = ctx.String("skiptables")
	opt.Keys = ctx.String("keys")
	opt.Parallel = ctx.Int("parallel")
	opt.MaxRecheckTimes = ctx.Int("max-recheck-times")
	opt.MaxRecheckRows = ctx.Int("max-recheck-rows")
	opt.Init()
	return &opt
}

func main() {
	//禁用颜色显示
	slog.Configure(func(logger *slog.SugaredLogger) {
		f := logger.Formatter.(*slog.TextFormatter)
		f.EnableColor = false
	})

	//诊断内存泄漏
	//go func() {
	//	_ = http.ListenAndServe(":9000", nil)
	//}()

	app := &cli.App{
		Name:  "./checkData",
		Usage: "check data tools",
		Commands: []*cli.Command{
			{
				Name:  "version",
				Usage: "show the version",
				Action: func(ctx *cli.Context) error {
					version()
					return nil
				},
			},
			{
				Name:  "mysql",
				Usage: "check data from mysql/doris/tidb",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "source", Aliases: []string{"S"}, Required: true, Usage: "The host and port of the source instance, e.g., 10.0.0.201:3306"},
					&cli.StringFlag{Name: "target", Aliases: []string{"T"}, Required: true, Usage: "The host and port of the target instance, e.g., 10.0.0.202:3306"},
					&cli.StringFlag{Name: "user", Aliases: []string{"u"}, Required: true, Usage: "Login user"},
					&cli.StringFlag{Name: "password", Aliases: []string{"p"}, Required: true, Usage: "Login password"},
					&cli.StringFlag{Name: "target-user", Aliases: []string{"tu"}, Usage: "Login user of target"},
					&cli.StringFlag{Name: "target-password", Aliases: []string{"tp"}, Usage: "Login password of target"},
					&cli.StringFlag{Name: "mode", Aliases: []string{"m"}, Value: "fast", Usage: "mode:[fast|slow|count]\n  fast: fast check,the database must surport crc32 functionn\n  slow: high compatibility, to work on doris/tidb\n  count: only check row count"},
					&cli.StringFlag{Name: "db", Aliases: []string{"d"}, Required: true, Usage: "dbname,e.g., db1,db2 or db1:db01,db2:db02(use a colon separate these diferent database names of the source and target)"},
					&cli.StringFlag{Name: "tables", Aliases: []string{"t"}, Usage: "These tables to check, e.g., users,orders"},
					&cli.StringFlag{Name: "where", Aliases: []string{"w"}, Usage: "filter condition, e.g., update_time<curdate()"},
					&cli.StringFlag{Name: "keys", Aliases: []string{"k"}, Usage: "These keys using to check, must be unique"},
					&cli.StringFlag{Name: "skip-tables", Usage: "These tables to skip check"},
					&cli.StringFlag{Name: "skip-cols", Usage: "These columns to skip check, to skip some big columns become faster"},
					&cli.IntFlag{Name: "parallel", Aliases: []string{"P"}, Value: 2, Usage: "Parallel"},
					&cli.IntFlag{Name: "max-recheck-times", Value: 3, Usage: "The number of recheck times"},
					&cli.IntFlag{Name: "max-recheck-rows", Value: 1000, Usage: "No recheck while the number of the found different during check greater than the max-recheck-rows valuse"},
				},
				Action: func(ctx *cli.Context) error {
					opt := GetOptions(ctx)
					opt.DbType = "mysql"
					check.Start(opt)
					return nil
				},
			},
			{
				Name:  "doris",
				Usage: "check data from doris",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "source", Aliases: []string{"S"}, Required: true, Usage: "The host and port of the source instance, e.g., 10.0.0.201:3306"},
					&cli.StringFlag{Name: "target", Aliases: []string{"T"}, Required: true, Usage: "The host and port of the target instance, e.g., 10.0.0.202:3306"},
					&cli.StringFlag{Name: "user", Aliases: []string{"u"}, Required: true, Usage: "Login user"},
					&cli.StringFlag{Name: "password", Aliases: []string{"p"}, Required: true, Usage: "Login password"},
					&cli.StringFlag{Name: "target-user", Aliases: []string{"tu"}, Usage: "Login user of target"},
					&cli.StringFlag{Name: "target-password", Aliases: []string{"tp"}, Usage: "Login password of target"},
					&cli.StringFlag{Name: "mode", Aliases: []string{"m"}, Value: "fast", Usage: "mode:[fast|slow|count]\n  fast: fast check,the database must surport crc32 functionn\n  slow: high compatibility, to work on doris/tidb\n  count: only check row count"},
					&cli.StringFlag{Name: "db", Aliases: []string{"d"}, Required: true, Usage: "dbname,e.g., db1,db2 or db1:db01,db2:db02(use a colon separate these diferent database names of the source and target)"},
					&cli.StringFlag{Name: "tables", Aliases: []string{"t"}, Usage: "These tables to check, e.g., users,orders"},
					&cli.StringFlag{Name: "where", Aliases: []string{"w"}, Usage: "filter condition, e.g., update_time<curdate()"},
					&cli.StringFlag{Name: "keys", Aliases: []string{"k"}, Usage: "These keys using to check, must be unique"},
					&cli.StringFlag{Name: "skip-tables", Usage: "These tables to skip check"},
					&cli.StringFlag{Name: "skip-cols", Usage: "These columns to skip check, to skip some big columns become faster"},
					&cli.IntFlag{Name: "parallel", Aliases: []string{"P"}, Value: 2, Usage: "Parallel"},
					&cli.IntFlag{Name: "max-recheck-times", Value: 3, Usage: "The number of recheck times"},
					&cli.IntFlag{Name: "max-recheck-rows", Value: 1000, Usage: "No recheck while the number of the found different during check greater than the max-recheck-rows valuse"},
				},
				Action: func(ctx *cli.Context) error {
					opt := GetOptions(ctx)
					opt.DbType = "doris"
					check.Start(opt)
					return nil
				},
			},
			{
				Name:  "oceanbase",
				Usage: "check data from oceanbase",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "source", Aliases: []string{"S"}, Required: true, Usage: "The host and port of the source instance, e.g., 10.0.0.201:3306"},
					&cli.StringFlag{Name: "target", Aliases: []string{"T"}, Required: true, Usage: "The host and port of the target instance, e.g., 10.0.0.202:3306"},
					&cli.StringFlag{Name: "user", Aliases: []string{"u"}, Required: true, Usage: "Login user"},
					&cli.StringFlag{Name: "password", Aliases: []string{"p"}, Required: true, Usage: "Login password"},
					&cli.StringFlag{Name: "target-user", Aliases: []string{"tu"}, Usage: "Login user of target"},
					&cli.StringFlag{Name: "target-password", Aliases: []string{"tp"}, Usage: "Login password of target"},
					&cli.StringFlag{Name: "mode", Aliases: []string{"m"}, Value: "fast", Usage: "mode:[fast|slow|count]\n  fast: fast check,the database must surport crc32 functionn\n  slow: high compatibility, to work on doris/tidb\n  count: only check row count"},
					&cli.StringFlag{Name: "db", Aliases: []string{"d"}, Required: true, Usage: "dbname,e.g., db1,db2 or db1:db01,db2:db02(use a colon separate these diferent database names of the source and target)"},
					&cli.StringFlag{Name: "tables", Aliases: []string{"t"}, Usage: "These tables to check, e.g., users,orders"},
					&cli.StringFlag{Name: "where", Aliases: []string{"w"}, Usage: "filter condition, e.g., update_time<curdate()"},
					&cli.StringFlag{Name: "keys", Aliases: []string{"k"}, Usage: "These keys using to check, must be unique"},
					&cli.StringFlag{Name: "skip-tables", Usage: "These tables to skip check"},
					&cli.StringFlag{Name: "skip-cols", Usage: "These columns to skip check, to skip some big columns become faster"},
					&cli.IntFlag{Name: "parallel", Aliases: []string{"P"}, Value: 2, Usage: "Parallel"},
					&cli.IntFlag{Name: "max-recheck-times", Value: 3, Usage: "The number of recheck times"},
					&cli.IntFlag{Name: "max-recheck-rows", Value: 1000, Usage: "No recheck while the number of the found different during check greater than the max-recheck-rows valuse"},
				},
				Action: func(ctx *cli.Context) error {
					opt := GetOptions(ctx)
					opt.DbType = "oceanbase"
					check.Start(opt)
					return nil
				},
			},
			{
				Name:  "mongo",
				Usage: "check data from mongo",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "source", Aliases: []string{"S"}, Required: true, Usage: "The host and port of the source instance, e.g., 10.0.0.201:27017"},
					&cli.StringFlag{Name: "target", Aliases: []string{"T"}, Required: true, Usage: "The host and port of the target instance, e.g., 10.0.0.202:27017"},
					&cli.StringFlag{Name: "user", Aliases: []string{"u"}, Required: true, Usage: "The login user which authenticationDatabase is admin"},
					&cli.StringFlag{Name: "password", Aliases: []string{"p"}, Required: true, Usage: "Login password"},
					&cli.StringFlag{Name: "target-user", Aliases: []string{"tu"}, Usage: "Login user of target"},
					&cli.StringFlag{Name: "target-password", Aliases: []string{"tp"}, Usage: "Login password of target"},
					&cli.StringFlag{Name: "mode", Aliases: []string{"m"}, Value: "fast", Usage: "mode:[fast|slow|count]\n  fast: fast check,the database must surport crc32 functionn\n  slow: high compatibility, to work on doris/tidb\n  count: only check row count"},
					&cli.StringFlag{Name: "db", Aliases: []string{"d"}, Required: true, Usage: "dbname,e.g., db1,db2 or db1:db01,db2:db02(use a colon separate these diferent database names of the source and target)"},
					&cli.StringFlag{Name: "tables", Aliases: []string{"t"}, Usage: "These tables to check, e.g., users,orders"},
					&cli.StringFlag{Name: "skip-tables", Usage: "These tables to skip check"},
					&cli.IntFlag{Name: "parallel", Aliases: []string{"P"}, Value: 2, Usage: "Parallel"},
					&cli.IntFlag{Name: "max-recheck-times", Value: 3, Usage: "The number of recheck times"},
					&cli.IntFlag{Name: "max-recheck-rows", Value: 1000, Usage: "No recheck while the number of the found different during check greater than the max-recheck-rows valuse"},
				},
				Action: func(ctx *cli.Context) error {
					opt := GetOptions(ctx)
					//执行主任务
					opt.DbType = "mongo"
					check.Start(opt)
					return nil

				},
			},
			{
				Name:  "pgsql",
				Usage: "check data from postgresql",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "source", Aliases: []string{"S"}, Required: true, Usage: "The host and port of the source instance, e.g., 10.0.0.201:5432"},
					&cli.StringFlag{Name: "target", Aliases: []string{"T"}, Required: true, Usage: "The host and port of the target instance, e.g., 10.0.0.202:5432"},
					&cli.StringFlag{Name: "user", Aliases: []string{"u"}, Required: true, Usage: "Login user"},
					&cli.StringFlag{Name: "password", Aliases: []string{"p"}, Required: true, Usage: "Login password"},
					&cli.StringFlag{Name: "target-user", Aliases: []string{"tu"}, Usage: "Login user of target"},
					&cli.StringFlag{Name: "target-password", Aliases: []string{"tp"}, Usage: "Login password of target"},
					&cli.StringFlag{Name: "mode", Aliases: []string{"m"}, Value: "fast", Usage: "mode:[fast|slow|count]\n  fast: fast check,the database must surport crc32 functionn\n  slow: high compatibility, to work on doris/tidb\n  count: only check row count"},
					&cli.StringFlag{Name: "db", Aliases: []string{"d"}, Required: true, Usage: "dbname,e.g., db1,db2 or db1:db01,db2:db02(use a colon separate these diferent database names of the source and target)"},
					&cli.StringFlag{Name: "tables", Aliases: []string{"t"}, Usage: "These full names to check table, e.g., public.users,public.orders"},
					&cli.StringFlag{Name: "where", Aliases: []string{"w"}, Usage: "filter condition, e.g., update_time<curdate()"},
					&cli.StringFlag{Name: "keys", Aliases: []string{"k"}, Usage: "These keys using to check, must be unique"},
					&cli.StringFlag{Name: "skip-tables", Usage: "These tables to skip check"},
					&cli.StringFlag{Name: "skip-cols", Usage: "These columns to skip check, to skip some big columns become faster"},
					&cli.IntFlag{Name: "parallel", Aliases: []string{"P"}, Value: 2, Usage: "Parallel"},
					&cli.IntFlag{Name: "max-recheck-times", Value: 3, Usage: "The number of recheck times"},
					&cli.IntFlag{Name: "max-recheck-rows", Value: 1000, Usage: "No recheck while the number of the found different during check greater than the max-recheck-rows valuse"},
				},
				Action: func(ctx *cli.Context) error {
					//初始化参数
					opt := GetOptions(ctx)
					//执行主任务
					opt.DbType = "pgsql"
					check.Start(opt)
					return nil
				},
			},

			{
				Name:  "mssql",
				Usage: "check data from sql server",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "source", Aliases: []string{"S"}, Required: true, Usage: "The host and port of the source instance, e.g., 10.0.0.201:1433"},
					&cli.StringFlag{Name: "target", Aliases: []string{"T"}, Required: true, Usage: "The host and port of the target instance, e.g., 10.0.0.202:1433"},
					&cli.StringFlag{Name: "user", Aliases: []string{"u"}, Required: true, Usage: "Login user"},
					&cli.StringFlag{Name: "password", Aliases: []string{"p"}, Required: true, Usage: "Login password"},
					&cli.StringFlag{Name: "target-user", Aliases: []string{"tu"}, Usage: "Login user of target"},
					&cli.StringFlag{Name: "target-password", Aliases: []string{"tp"}, Usage: "Login password of target"},
					&cli.StringFlag{Name: "mode", Aliases: []string{"m"}, Value: "fast", Usage: "mode:[fast|slow|count]\n  fast: fast check,the database must surport crc32 functionn\n  slow: high compatibility, to work on doris/tidb\n  count: only check row count"},
					&cli.StringFlag{Name: "db", Aliases: []string{"d"}, Required: true, Usage: "dbname,e.g., db1,db2 or db1:db01,db2:db02(use a colon separate these diferent database names of the source and target)"},
					&cli.StringFlag{Name: "tables", Aliases: []string{"t"}, Usage: "These full names to check table, e.g., public.users,public.orders"},
					&cli.StringFlag{Name: "where", Aliases: []string{"w"}, Usage: "filter condition, e.g., update_time<curdate()"},
					&cli.StringFlag{Name: "keys", Aliases: []string{"k"}, Usage: "These keys using to check, must be unique"},
					&cli.StringFlag{Name: "skip-tables", Usage: "These tables to skip check"},
					&cli.StringFlag{Name: "skip-cols", Usage: "These columns to skip check, to skip some big columns become faster"},
					&cli.IntFlag{Name: "parallel", Aliases: []string{"P"}, Value: 2, Usage: "Parallel"},
					&cli.IntFlag{Name: "max-recheck-times", Value: 3, Usage: "The number of recheck times"},
					&cli.IntFlag{Name: "max-recheck-rows", Value: 1000, Usage: "No recheck while the number of the found different during check greater than the max-recheck-rows valuse"},
				},
				Action: func(ctx *cli.Context) error {
					//初始化参数
					opt := GetOptions(ctx)
					//执行主任务
					opt.DbType = "mssql"
					check.Start(opt)
					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
