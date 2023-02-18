package main

import (
	"checkData/check"
	"checkData/util"
	"fmt"
	"github.com/gookit/slog"
	"github.com/urfave/cli/v2"
	"log"
	"os"
	"strings"
)

func version() {
	text := `
####################################################################################################
#  Name        :  checkData
#  Author      :  Elison
#  Email       :  Ly99@qq.com
#  Description :  核对迁移或同步的数据库两端的数据是否一致,支持mysql(tidb/doris)、mongo等多种数据库
#  Updates     :
#      Version     When            What
#      --------    -----------     -----------------------------------------------------------------
#      v1.0        2017-08-18      python初版
#      v2.0        2022-02-25      替换sqlite3，使用diff对比差异
#      v3.0        2023-01-20      优化性能，使用golang替换python
#      v3.1.0      2023-02-10      为了兼容其他数据库，使用interface重构Checker代码
#      v3.1.1      2023-02-12      增加mongo核对功能
#      v3.1.2      2023-02-13      增加pgsql核对功能
####################################################################################################
`
	fmt.Println(text)
}

func GetOptions(ctx *cli.Context) util.Options {
	opt := util.Options{}
	opt.Source = ctx.String("source")
	opt.Target = ctx.String("target")
	opt.User = ctx.String("user")
	opt.Password = ctx.String("password")
	opt.Mode = ctx.String("mode")
	opt.Db = ctx.String("db")
	opt.Tables = ctx.String("tables")
	opt.Where = ctx.String("where")
	opt.SkipCols = ctx.String("skipcols")
	opt.Keys = ctx.String("keys")
	opt.Parallel = ctx.Int("parallel")
	opt.MaxRecheckRows = ctx.Int("max-recheck-rows")
	opt.CheckOption()
	return opt
}

func main() {
	//禁用颜色显示
	slog.Configure(func(logger *slog.SugaredLogger) {
		f := logger.Formatter.(*slog.TextFormatter)
		f.EnableColor = false
	})

	app := &cli.App{
		Name:  "./checkData",
		Usage: "check data tools",
		Commands: []*cli.Command{
			{
				Name:  "version",
				Usage: "check data from mongo",
				Action: func(ctx *cli.Context) error {
					version()
					return nil
				},
			},
			{
				Name:  "mysql",
				Usage: "check data from mysql/doris/tidb",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "source", Aliases: []string{"S"}, Required: true, Usage: "The host and port of the source instance, eq: 10.0.0.201:3306"},
					&cli.StringFlag{Name: "target", Aliases: []string{"T"}, Required: true, Usage: "The host and port of the target instance, eq: 10.0.0.202:3306"},
					&cli.StringFlag{Name: "user", Aliases: []string{"u"}, Required: true, Usage: "Login user"},
					&cli.StringFlag{Name: "password", Aliases: []string{"p"}, Required: true, Usage: "Login password"},
					&cli.StringFlag{Name: "mode", Aliases: []string{"m"}, Value: "fast", Usage: "mode:[fast|slow|count]\n  fast: fast check,the database must surport crc32 functionn\n  slow: high compatibility, to work on doris/tidb\n  count: only check row count"},
					&cli.StringFlag{Name: "db", Aliases: []string{"d"}, Required: true, Usage: "dbname,eq: db1,db2 or db1:db01,db2:db02(use a colon separate these diferent database names of the source and target)"},
					&cli.StringFlag{Name: "tables", Aliases: []string{"t"}, Usage: "These tables to check, eq: users,orders"},
					&cli.StringFlag{Name: "where", Aliases: []string{"w"}, Usage: "filter condition, eq: update_time<curdate()"},
					&cli.StringFlag{Name: "keys", Aliases: []string{"k"}, Usage: "These keys using to check, must be unique"},
					&cli.StringFlag{Name: "skipcols", Usage: "These columns to skip check, to skip some big columns become faster"},
					&cli.IntFlag{Name: "parallel", Aliases: []string{"P"}, Value: 2, Usage: "Parallel"},
					&cli.IntFlag{Name: "max-recheck-rows", Value: 1000, Usage: "No recheck while the number of the found different during check greater than the max-recheck-rows valuse"},
				},
				Action: func(ctx *cli.Context) error {
					//初始化参数
					opt := GetOptions(ctx)
					//执行主任务
					for _, dbstr := range opt.DbList {
						var dbgroup [2]string
						g := strings.Split(dbstr, ":")
						if len(g) == 2 {
							dbgroup[0] = g[0]
							dbgroup[1] = g[1]
						} else {
							dbgroup[0] = g[0]
							dbgroup[1] = g[0]
						}
						check.CheckMysqlDB(opt, dbgroup)
					}

					return nil
				},
			},
			{
				Name:  "mongo",
				Usage: "check data from mongo",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "source", Aliases: []string{"S"}, Required: true, Usage: "The host and port of the source instance, eq: 10.0.0.201:27017"},
					&cli.StringFlag{Name: "target", Aliases: []string{"T"}, Required: true, Usage: "The host and port of the target instance, eq: 10.0.0.202:27017"},
					&cli.StringFlag{Name: "user", Aliases: []string{"u"}, Required: true, Usage: "The login user which authenticationDatabase is admin"},
					&cli.StringFlag{Name: "password", Aliases: []string{"p"}, Required: true, Usage: "Login password"},
					&cli.StringFlag{Name: "mode", Aliases: []string{"m"}, Value: "fast", Usage: "mode:[fast|slow|count]\n  fast: fast check,the database must surport crc32 functionn\n  slow: high compatibility, to work on doris/tidb\n  count: only check row count"},
					&cli.StringFlag{Name: "db", Aliases: []string{"d"}, Required: true, Usage: "dbname,eq: db1,db2 or db1:db01,db2:db02(use a colon separate these diferent database names of the source and target)"},
					&cli.StringFlag{Name: "tables", Aliases: []string{"t"}, Usage: "These tables to check, eq: users,orders"},
					&cli.IntFlag{Name: "parallel", Aliases: []string{"P"}, Value: 2, Usage: "Parallel"},
					&cli.IntFlag{Name: "max-recheck-rows", Value: 1000, Usage: "No recheck while the number of the found different during check greater than the max-recheck-rows valuse"},
				},
				Action: func(ctx *cli.Context) error {
					opt := GetOptions(ctx)
					//执行主任务
					for _, dbstr := range opt.DbList {
						var dbgroup [2]string
						g := strings.Split(dbstr, ":")
						if len(g) == 2 {
							dbgroup[0] = g[0]
							dbgroup[1] = g[1]
						} else {
							dbgroup[0] = g[0]
							dbgroup[1] = g[0]
						}

						check.CheckMongoDB(opt, dbgroup)
					}

					return nil

				},
			},
			{
				Name:  "pgsql",
				Usage: "check data for postgresql",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "source", Aliases: []string{"S"}, Required: true, Usage: "The host and port of the source instance, eq: 10.0.0.201:5432"},
					&cli.StringFlag{Name: "target", Aliases: []string{"T"}, Required: true, Usage: "The host and port of the target instance, eq: 10.0.0.202:5432"},
					&cli.StringFlag{Name: "user", Aliases: []string{"u"}, Required: true, Usage: "Login user"},
					&cli.StringFlag{Name: "password", Aliases: []string{"p"}, Required: true, Usage: "Login password"},
					&cli.StringFlag{Name: "mode", Aliases: []string{"m"}, Value: "fast", Usage: "mode:[fast|slow|count]\n  fast: fast check,the database must surport crc32 functionn\n  slow: high compatibility, to work on doris/tidb\n  count: only check row count"},
					&cli.StringFlag{Name: "db", Aliases: []string{"d"}, Required: true, Usage: "dbname,eq: db1,db2 or db1:db01,db2:db02(use a colon separate these diferent database names of the source and target)"},
					&cli.StringFlag{Name: "tables", Aliases: []string{"t"}, Usage: "These tables to check, eq: users,orders"},
					&cli.StringFlag{Name: "where", Aliases: []string{"w"}, Usage: "filter condition, eq: update_time<curdate()"},
					&cli.StringFlag{Name: "keys", Aliases: []string{"k"}, Usage: "These keys using to check, must be unique"},
					&cli.StringFlag{Name: "skipcols", Usage: "These columns to skip check, to skip some big columns become faster"},
					&cli.IntFlag{Name: "parallel", Aliases: []string{"P"}, Value: 2, Usage: "Parallel"},
					&cli.IntFlag{Name: "max-recheck-rows", Value: 1000, Usage: "No recheck while the number of the found different during check greater than the max-recheck-rows valuse"},
				},
				Action: func(ctx *cli.Context) error {
					//初始化参数
					opt := GetOptions(ctx)
					//执行主任务
					for _, dbstr := range opt.DbList {
						var dbgroup [2]string
						g := strings.Split(dbstr, ":")
						if len(g) == 2 {
							dbgroup[0] = g[0]
							dbgroup[1] = g[1]
						} else {
							dbgroup[0] = g[0]
							dbgroup[1] = g[0]
						}

						check.CheckPgsqlDB(opt, dbgroup)
					}

					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
