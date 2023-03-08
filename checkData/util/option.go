package util

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Options struct {
	Source         string
	Target         string
	User           string
	Password       string
	Mode           string
	Db             string
	Tables         string
	SkipCols       string
	SkipTables     string
	SourceHost     string
	SourcePort     int
	TargetHost     string
	TargetPort     int
	Where          string
	Keys           string
	DbList         []string
	TableList      []string
	SkipColList    []string
	SkipTableList  []string
	KeysList       []string
	Parallel       int
	MaxRecheckRows int
}

func (this *Options) Init() {
	//this.setOption()
	this.InitOption()
}

//func (this *Options) setOption() {
//	flag.StringVar(&this.Source, "source", "", "source实例IP和端口,如: 10.0.0.201:3306")
//	flag.StringVar(&this.Target, "target", "", "target实例IP和端口,如: 10.0.0.202:3306")
//	flag.StringVar(&this.User, "user", "", "数据库用户")
//	flag.StringVar(&this.Password, "password", "", "数据库密码")
//	flag.StringVar(&this.Mode, "mode", "default", "核对模式，default:默认模式,fast:只核对行数,slow:兼容模式（支持Mysql\\Doris\\Tidb混合核对）")
//	flag.StringVar(&this.Db, "db", "", "要核对的数据库，如：db1,db2 或 db1:db01,db2:db02(源端和目标端库名不一致时可以使用冒号分隔)")
//	flag.StringVar(&this.Tables, "tables", "", "要核对的表(不指定为全库核对，多个表使用英文逗号分隔)，如：users,orders")
//	flag.StringVar(&this.Where, "where", "", "过滤条件，不设置则全量核对，如：update_time<curdate()")
//	flag.StringVar(&this.Keys, "keys", "", "核对使用的键，必须唯一，默认：主键")
//	flag.StringVar(&this.SkipCols, "skip-cols", "", "过滤不需要核对的列，如：remark,contents")
//	flag.IntVar(&this.Parallel, "parallel", 2, "并发数，默认：2")
//	flag.IntVar(&this.MaxRecheckRows, "max-recheck-rows", 100, "最大复核行数，不一致的行数超过该数值，不进行复核，默认：100")
//	flag.Parse()
//}

func (this *Options) InitOption() {

	//处理source参数
	s := strings.Split(this.Source, ":")
	if len(s) == 2 {
		this.SourceHost = s[0]
		port, _ := strconv.ParseUint(s[1], 10, 64)
		this.SourcePort = int(port)
	}

	//处理target参数
	t := strings.Split(this.Target, ":")
	if len(t) == 2 {
		this.TargetHost = t[0]
		port, _ := strconv.ParseUint(t[1], 10, 64)
		this.TargetPort = int(port)
	}
	if this.SourcePort == 0 || this.TargetPort == 0 {
		fmt.Println("source或target参数无效")
		os.Exit(1)
	}

	//用户账号
	if this.User == "" || this.Password == "" {
		fmt.Println("用户名或密码无效")
		os.Exit(1)
	}

	//处理db参数
	if this.Db != "" {
		this.DbList = strings.Split(this.Db, ",")
	} else {
		fmt.Println("db参数无效:", this.Db)
		os.Exit(1)
	}

	//处理tables参数
	if this.Tables != "" {
		this.TableList = strings.Split(this.Tables, ",")
	}

	//处理SkipCols参数
	if this.SkipCols != "" {
		this.SkipColList = strings.Split(this.SkipCols, ",")
	}

	//处理SkipTables参数
	if this.SkipTables != "" {
		this.SkipTableList = strings.Split(this.SkipTables, ",")
	}

	//keys
	if this.Keys != "" {
		this.KeysList = strings.Split(this.Keys, ",")
	}

}
