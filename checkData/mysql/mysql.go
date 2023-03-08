package mysql

import (
	"checkData/util"
	"fmt"
	"github.com/gookit/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CheckData struct {
	Id  string
	Sum uint32
}

type MysqlDatabase struct {
	Db               string
	SourceDb         string
	TargetDb         string
	SourceHost       string
	SourcePort       int
	TargetHost       string
	TargetPort       int
	SourceDbConn     *util.MysqlDb
	TargetDbConn     *util.MysqlDb
	SourceTables     []string
	TargetTables     []string
	ToCheckTables    []string
	SourceMoreTables []string
	TargetMoreTables []string
	SkipTables       []string
	ReportFile       string
	Logdir           string
}

func (this *MysqlDatabase) Init(opt util.Options, dbgroup [2]string) {
	this.SourceDb = dbgroup[0]
	this.TargetDb = dbgroup[1]
	this.Db = this.SourceDb
	this.SourceHost = opt.SourceHost
	this.TargetHost = opt.TargetHost
	this.SourcePort = opt.SourcePort
	this.TargetPort = opt.TargetPort
	this.SourceDbConn = &util.MysqlDb{Host: this.SourceHost, Port: this.SourcePort, User: opt.User, Password: opt.Password, Database: this.SourceDb}
	this.TargetDbConn = &util.MysqlDb{Host: this.TargetHost, Port: this.TargetPort, User: opt.User, Password: opt.Password, Database: this.TargetDb}
	slog.Infof("[%s:%s] 开启数据库连接池", this.SourceDb, this.TargetDb)
	this.SourceDbConn.Init()
	this.TargetDbConn.Init()
	this.ReportFile = fmt.Sprintf("%s_%d/%s.txt", this.SourceHost, opt.SourcePort, dbgroup[0])
	this.Logdir = fmt.Sprintf("%s_%d/%s", opt.SourceHost, opt.SourcePort, dbgroup[0])
	this.ToCheckTables = opt.TableList
	this.SkipTables = opt.SkipTableList
	err := os.MkdirAll(this.Logdir, 0775)
	if err != nil {
		slog.Errorf("创建目录%s报错: %s", this.Logdir, err)
	}

}

func (this *MysqlDatabase) GetTables() {
	// 获取表名
	sql := fmt.Sprintf("show tables")
	//获取源库所有表
	srows, err := this.SourceDbConn.QueryReturnList(sql)
	if err != nil {
		slog.Errorf("获取%s库的表名失败，%s", this.SourceDb, err)
	}
	//保存
	for _, v := range srows {
		this.SourceTables = append(this.SourceTables, v[0])
	}

	//获取目标库所有表
	trows, err := this.TargetDbConn.QueryReturnList(sql)
	if err != nil {
		slog.Errorf("获取%s库的表名失败，%s", this.TargetDb, err)
	}
	//保存
	for _, v := range trows {
		this.TargetTables = append(this.TargetTables, v[0])
	}

}

func (this *MysqlDatabase) GetToCheckTables() {
	//获取两端都存在的表

	if len(this.ToCheckTables) == 0 {
		this.GetTables()

		//目标库不存在的表
		for _, t := range this.SourceTables {
			if !util.InArray(this.TargetTables, t) {
				this.SourceMoreTables = append(this.SourceMoreTables, t)
			} else {
				this.ToCheckTables = append(this.ToCheckTables, t)
			}
		}

		//源库不存在的表
		for _, t := range this.TargetTables {
			if !util.InArray(this.SourceTables, t) {
				this.TargetMoreTables = append(this.TargetMoreTables, t)
			}
		}

		//过滤不需要检查的表
		if len(this.SkipTables) > 0 {
			var tbs []string
			for _, tb := range this.ToCheckTables {
				if !util.InArray(this.SkipTables, tb) {
					tbs = append(tbs, tb)
				}
			}
			this.ToCheckTables = tbs
		}
	}
}

func (this *MysqlDatabase) Close() {
	//关闭连接池
	this.SourceDbConn.Close()
	this.TargetDbConn.Close()
	slog.Infof("[%s:%s] 关闭数据库连接池", this.SourceDb, this.TargetDb)
}

type MysqlTable struct {
	DbName          string
	TbName          string
	Mode            string //fast,slow,count
	Keys            []string
	Columns         []string
	Where           string
	SkipColumns     []string
	SqlText         string
	SourceChan      chan CheckData
	TargetChan      chan CheckData
	Result          string //检查结果 yes/no/unknown
	Error           string
	SourceRows      int
	TargetRows      int
	SameRows        int
	DiffRows        int
	SourceMoreRows  int
	TargetMoreRows  int
	MaxRecheckRows  int
	RecheckPassRows int
	ExecuteSeconds  int
	DbGroup         *MysqlDatabase
}

func NewMysqlTable(tb string, dbg *MysqlDatabase, opt util.Options) *MysqlTable {
	//MysqlTable构造函数

	return &MysqlTable{
		DbName:         dbg.Db,
		TbName:         tb,
		Mode:           opt.Mode,
		SkipColumns:    opt.SkipColList,
		Keys:           opt.KeysList,
		Where:          opt.Where,
		MaxRecheckRows: opt.MaxRecheckRows,
		DbGroup:        dbg,
	}
}

func (this *MysqlTable) Init() {
	slog.Infof("[%s.%s] 开始初始化 mode=%s", this.DbName, this.TbName, this.Mode)
	this.SourceChan = make(chan CheckData, 1000)
	this.TargetChan = make(chan CheckData, 1000)
	this.RecheckPassRows = -1
}

func (this *MysqlTable) GetName() string {
	//获取表名
	return this.DbName + "." + this.TbName
}

func (this *MysqlTable) GetTableColumns() {
	// 获取表列名
	defer func() {
		if msg := recover(); msg != nil {
			this.Result = "unknown"
			this.Error = fmt.Sprintf("GetTableColumns panic, %s", msg)
			slog.Error(this.Error)
		}
	}()
	pk := []string{}
	cols := []string{}
	skipcols := []string{}
	sql := fmt.Sprintf("desc `%s`", this.TbName)
	rows, err := this.DbGroup.SourceDbConn.QueryReturnList(sql)

	if err != nil {
		panic(err)
	}

	for _, row := range rows {
		//找主键
		if row[3] == "PRI" {
			pk = append(pk, row[0])
		}

		//找列
		if util.InArray(this.SkipColumns, row[0]) {
			//跳过不需要核对的列
			skipcols = append(skipcols, row[0])
		} else {
			cols = append(cols, row[0])
		}

	}
	this.Columns = cols
	if len(this.Keys) == 0 {
		this.Keys = pk
	}
	slog.Infof("[%s.%s] 主键列: %s", this.DbName, this.TbName, strings.Join(this.Keys, ", "))
	if len(skipcols) > 0 {
		slog.Infof("[%s.%s] 跳过不需要核对的列: %s", this.DbName, this.TbName, strings.Join(skipcols, ", "))
	}
}

func (this *MysqlTable) GetCRC32DataSQL() {
	//生成计算CRC32数据的SQL
	var sql string
	KeysText := strings.Join(util.EncloseStringArray(this.Keys, "`"), ", ")
	ColumnsText := strings.Join(util.EncloseStringArray(this.Columns, "`"), ", ")
	if this.Mode == "count" {
		this.SqlText = fmt.Sprintf("select count(*) cnt from `%s`", this.TbName)
	} else {
		if this.Mode == "slow" {
			sql = fmt.Sprintf("select concat_ws(',',%s) pk,concat_ws('|',%s) as rowdata from `%s`", KeysText, ColumnsText, this.TbName)
		} else {
			sql = fmt.Sprintf("select concat_ws(',',%s) pk,crc32(concat_ws('|',%s)) chksum from `%s`", KeysText, ColumnsText, this.TbName)
		}

		if this.Where != "" {
			sql += " where " + this.Where
		}
		this.SqlText = sql + " order by " + KeysText
	}

	slog.Infof("[%s.%s] SqlText: %s", this.DbName, this.TbName, this.SqlText)
}

func (this *MysqlTable) GetMode() string {
	return this.Mode
}

func (this *MysqlTable) Precheck() bool {
	//预检查
	slog.Infof("[%s.%s] 执行预检查", this.DbName, this.TbName)

	//获取列名
	this.GetTableColumns()
	if this.Error == "" {
		this.GetCRC32DataSQL()
	}

	if this.Mode == "count" && this.SqlText != "" {
		return true
	}

	//预检查
	if len(this.Columns) == 0 {
		this.Result = "unknown"
		this.Error = "获取列名失败"
		slog.Errorf("[%s.%s] 获取列名失败，跳过核对", this.DbName, this.TbName)
	} else if len(this.Keys) == 0 {
		this.Result = "unknown"
		this.Error = "没有主键"
		slog.Errorf("[%s.%s] 没有主键，跳过核对", this.DbName, this.TbName)
	} else if this.SqlText == "" {
		this.Result = "unknown"
		slog.Errorf("[%s.%s] 获取SqlText失败，跳过核对", this.DbName, this.TbName)
	} else {
		return true
	}
	slog.Infof("[%s.%s] 预检查不通过", this.DbName, this.TbName)
	return false
}

func (this *MysqlTable) GetSourceCRC32Data(stopSource, stopTarget chan struct{}) {
	//获取源端数据，在数据库上计算CRC32，性能高

	defer util.TimeCost()(fmt.Sprintf("[%s.%s] Source端数据下载完成", this.DbGroup.SourceDb, this.TbName))
	defer func() {
		if msg := recover(); msg != nil {
			defer func() {
				recover() //捕获外层defer产生的panic，如 panic: send on closed channel
			}()
			this.Result = "unknown"
			this.Error = fmt.Sprintf("GetSourceCRC32Data panic, %s", msg)
			slog.Error(this.Error)
			stopTarget <- struct{}{} //发送停止信号给target
		}
	}()
	defer close(this.SourceChan)
	var data CheckData
	slog.Infof("[%s.%s] 开始下载source端数据", this.DbGroup.SourceDb, this.TbName)
	cur, err := this.DbGroup.SourceDbConn.ConnPool.Query(this.SqlText)
	if err != nil {
		panic(err)
	}
	defer cur.Close() //当连接中断，这个操作会卡住60s+

	for cur.Next() {
		err := cur.Scan(&data.Id, &data.Sum)
		if err != nil {
			panic(err)
		}
		select {
		case this.SourceChan <- data:
			this.SourceRows++
		case <-stopSource:
			slog.Infof("收到停止信号，结束数据下载[%s.%s]", this.DbGroup.SourceDb, this.TbName)
			return
		}
	}
}

func (this *MysqlTable) GetTargetCRC32Data(stopSource, stopTarget chan struct{}) {
	//获取目标端数据，在数据库上计算CRC32，性能高
	defer util.TimeCost()(fmt.Sprintf("[%s.%s] Target端数据下载完成", this.DbGroup.TargetDb, this.TbName))
	defer func() {
		if msg := recover(); msg != nil {
			defer func() {
				recover() //捕获外层defer产生的panic，如 panic: send on closed channel
			}()
			this.Result = "unknown"
			this.Error = fmt.Sprintf("GetTargetCRC32Data panic, %s", msg)
			slog.Error(this.Error)
			stopSource <- struct{}{} //发送停止信号给source
		}
	}()
	defer close(this.TargetChan)

	var data CheckData
	slog.Infof("[%s.%s] 开始下载target端数据", this.DbGroup.TargetDb, this.TbName)
	cur, err := this.DbGroup.TargetDbConn.ConnPool.Query(this.SqlText)
	if err != nil {
		panic(err)
	}
	defer cur.Close() //当连接中断，这个操作会卡住60s+

	for cur.Next() {
		err := cur.Scan(&data.Id, &data.Sum)
		if err != nil {
			panic(err)
		}
		select {
		case this.TargetChan <- data:
			this.TargetRows++
		case <-stopTarget:
			slog.Infof("收到停止信号，结束数据下载[%s.%s]", this.DbGroup.TargetDb, this.TbName)
			return
		}
	}
}

func (this *MysqlTable) GetSourceCRC32DataSlow(stopSource, stopTarget chan struct{}) {
	// 获取源端数据，在本地计算CRC32，速度慢
	defer util.TimeCost()(fmt.Sprintf("[%s.%s] Source端数据下载完成", this.DbGroup.SourceDb, this.TbName))
	defer func() {
		if msg := recover(); msg != nil {
			defer func() {
				recover() //捕获外层defer产生的panic，如 panic: send on closed channel
			}()
			this.Result = "unknown"
			this.Error = fmt.Sprintf("GetSourceCRC32DataSlow panic, %s", msg)
			slog.Error(this.Error)
			stopTarget <- struct{}{} //发送停止信号给target
		}
	}()
	defer close(this.SourceChan)

	var (
		id   string
		text string
		data CheckData
	)
	slog.Infof("[%s.%s] 开始下载source端数据", this.DbGroup.SourceDb, this.TbName)
	cur, err := this.DbGroup.SourceDbConn.ConnPool.Query(this.SqlText)
	if err != nil {
		panic(err)
	}
	defer cur.Close()

	for cur.Next() {
		err := cur.Scan(&id, &text)
		if err != nil {
			panic(err)
		}
		data = CheckData{Id: id, Sum: util.CRC32(text)}
		select {
		case this.SourceChan <- data:
			this.SourceRows++
		case <-stopSource:
			slog.Infof("收到停止信号，结束数据下载[%s.%s]", this.DbGroup.TargetDb, this.TbName)
			return
		}
	}
}

func (this *MysqlTable) GetTargetCRC32DataSlow(stopSource, stopTarget chan struct{}) {
	// 获取目标端数据，在本地计算CRC32，速度慢

	defer util.TimeCost()(fmt.Sprintf("[%s.%s] Target端数据下载完成", this.DbGroup.TargetDb, this.TbName))
	defer func() {
		if msg := recover(); msg != nil {
			defer func() {
				recover() //捕获外层defer产生的panic，如 panic: send on closed channel
			}()
			this.Result = "unknown"
			this.Error = fmt.Sprintf("GetTargetCRC32DataSlow panic, %s", msg)
			slog.Error(this.Error)
			stopSource <- struct{}{} //发送停止信号给source
		}
	}()
	defer close(this.TargetChan)

	var (
		id   string
		text string
		data CheckData
	)
	slog.Infof("[%s.%s] 开始下载target端数据", this.DbGroup.TargetDb, this.TbName)
	cur, err := this.DbGroup.TargetDbConn.ConnPool.Query(this.SqlText)
	if err != nil {
		panic(err)
	}
	defer cur.Close()

	for cur.Next() {
		err := cur.Scan(&id, &text)
		if err != nil {
			panic(err)
		}
		data = CheckData{Id: id, Sum: util.CRC32(text)}
		select {
		case this.TargetChan <- data:
			this.TargetRows++
		case <-stopTarget:
			slog.Infof("收到停止信号，结束数据下载[%s.%s]", this.DbGroup.TargetDb, this.TbName)
			return
		}
	}
}

func (this *MysqlTable) GetSourceTableCount(wg *sync.WaitGroup) {
	// 返回表总行数
	defer util.TimeCost()(fmt.Sprintf("[%s.%s] Source端总行数统计完成", this.DbGroup.SourceDb, this.TbName))
	defer (*wg).Done()
	defer func() {
		if msg := recover(); msg != nil {
			this.Result = "unknown"
			this.Error = fmt.Sprintf("CheckSourceTableCount panic, %s", msg)
			slog.Error(this.Error)
		}
	}()
	rows, err := this.DbGroup.SourceDbConn.QueryReturnList(this.SqlText)
	if err != nil {
		panic(err)
	} else {
		cnt, _ := strconv.Atoi(rows[0][0])
		this.SourceRows = cnt
	}
}

func (this *MysqlTable) GetTargetTableCount(wg *sync.WaitGroup) {
	// 返回表总行数
	defer util.TimeCost()(fmt.Sprintf("[%s.%s] Target端总行数统计完成", this.DbGroup.TargetDb, this.TbName))
	defer (*wg).Done()
	defer func() {
		if msg := recover(); msg != nil {
			this.Result = "unknown"
			this.Error = fmt.Sprintf("CheckTargetTableCount panic, %s", msg)
			slog.Error(this.Error)
		}
	}()
	rows, err := this.DbGroup.TargetDbConn.QueryReturnList(this.SqlText)
	if err != nil {
		panic(err)
	} else {
		cnt, _ := strconv.Atoi(rows[0][0])
		this.TargetRows = cnt
	}
}

func (this *MysqlTable) GetRepairSQL(idtext string, mode int) (string, error) {
	// 生成replace sql
	// mode:修复模式, 0-delete 1-insert 2-replace
	var sql, where string
	ColumnsText := strings.Join(util.EncloseStringArray(this.Columns, "`"), ", ")

	//拼接where
	idValues := strings.Split(idtext, ",")
	for i, v := range this.Keys {
		if i > 0 {
			where += fmt.Sprintf(" and `%s`='%s'", v, idValues[i])
		} else {
			where += fmt.Sprintf("`%s`='%s'", v, idValues[i])
		}
	}

	sqltext := ""
	if mode == 0 {
		//拼接SQL
		sqltext = fmt.Sprintf("delete from `%s` where %s;\n", this.TbName, where)
	} else {
		// 查询数据
		sql = fmt.Sprintf("select %s from `%s` where %s;\n", ColumnsText, this.TbName, where)
		rows, err := this.DbGroup.SourceDbConn.QueryReturnText(sql)
		if err != nil {
			return "", err
		}

		//拼接SQL
		for _, v := range rows {
			if mode == 1 {
				sqltext += fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s;\n", this.TbName, ColumnsText, v)
			} else {
				sqltext += fmt.Sprintf("REPLACE INTO `%s` (%s) VALUES %s;\n", this.TbName, ColumnsText, v)
			}
		}
	}
	return sqltext, nil
}

func (this *MysqlTable) CheckTableCount() {
	// 检查表总行数
	defer util.TimeCost()(fmt.Sprintf("[%s.%s] 表行数核对完成", this.DbName, this.TbName))
	defer func() {
		if msg := recover(); msg != nil {
			this.Result = "unknown"
			this.Error = fmt.Sprintf("CheckTableCount panic, %s", msg)
			slog.Error(this.Error)
		}
	}()
	ts := time.Now().Unix()
	var wg sync.WaitGroup
	wg.Add(2)
	go this.GetSourceTableCount(&wg)
	go this.GetTargetTableCount(&wg)

	//等待完成
	wg.Wait()

	//对比行数
	if this.Result == "unknown" {
		text := fmt.Sprintf("[%s.%s] [Mode:count Result:%s SourceRows:%d TargetRows:%d]", this.DbName, this.TbName, this.Result, this.SourceRows, this.TargetRows)
		slog.Info(text)
	} else if this.SourceRows == this.TargetRows {
		this.Result = "yes"
		text := fmt.Sprintf("[%s.%s] [Mode:count Result:%s SourceRows:%d TargetRows:%d]", this.DbName, this.TbName, this.Result, this.SourceRows, this.TargetRows)
		slog.Info(text)
	} else {
		this.Result = "no"
		text := fmt.Sprintf("[%s.%s] [Mode:count Result:%s SourceRows:%d TargetRows:%d]", this.DbName, this.TbName, this.Result, this.SourceRows, this.TargetRows)
		slog.Info(text)
	}

	this.ExecuteSeconds = int(time.Now().Unix() - ts)

}

func (this *MysqlTable) CheckTableDetail() {
	// 开始核对行明细

	defer util.TimeCost()(fmt.Sprintf("[%s.%s] 表数据核对完成", this.DbName, this.TbName))
	stopSource := make(chan struct{}, 2)
	stopTarget := make(chan struct{}, 2)
	defer close(stopSource)
	defer close(stopTarget)

	ts := time.Now().Unix()
	diffList := make([]string, 0)
	SourceMoreDict := make(map[string]uint32, 10000)
	TargetMoreDict := make(map[string]uint32, 10000)

	defer func() {

		text := fmt.Sprintf("[%s.%s] [Result:%s SourceRows:%d TargetRows:%d SameRows:%d DiffRows:%d SourceMoreRows:%d TargetMoreRows:%d RecheckPassRows:%d]", this.DbName, this.TbName, this.Result, this.SourceRows, this.TargetRows, this.SameRows, this.DiffRows, this.SourceMoreRows, this.TargetMoreRows, this.RecheckPassRows)
		slog.Info(text)

		//保存不一致的数据id
		if len(diffList) > 0 {
			diffFileName := fmt.Sprintf("%s/%s.diff", this.DbGroup.Logdir, this.TbName)
			diffFile, err := util.File(diffFileName)
			if err != nil {
				slog.Errorf("写入文件%s报错: %s", diffFileName, err)
			} else {
				for _, id := range diffList {
					diffFile.WriteString(id + "\n")
				}
				diffFile.Close()
			}
		}

		if len(SourceMoreDict) > 0 {
			tlossFileName := fmt.Sprintf("%s/%s.tlost", this.DbGroup.Logdir, this.TbName)
			tlossFile, err := util.File(tlossFileName)
			if err != nil {
				slog.Errorf("写入文件%s报错: %s", tlossFileName, err)
			} else {
				for id, _ := range SourceMoreDict {
					tlossFile.WriteString(id + "\n")
				}
				tlossFile.Close()
			}
		}

		if len(TargetMoreDict) > 0 {
			tmoreFileName := fmt.Sprintf("%s/%s.tmore", this.DbGroup.Logdir, this.TbName)
			tmoreFile, err := util.File(tmoreFileName)
			if err != nil {
				slog.Errorf("写入文件%s报错: %s", tmoreFileName, err)
			} else {
				for id, _ := range TargetMoreDict {
					tmoreFile.WriteString(id + "\n")
				}
				tmoreFile.Close()
			}
		}
	}()

	if this.Mode == "slow" {
		go this.GetSourceCRC32DataSlow(stopSource, stopTarget)
		go this.GetTargetCRC32DataSlow(stopSource, stopTarget)
	} else {
		go this.GetSourceCRC32Data(stopSource, stopTarget)
		go this.GetTargetCRC32Data(stopSource, stopTarget)
	}

	sdata, schIsOk := <-this.SourceChan
	tdata, tchIsOk := <-this.TargetChan

	for schIsOk && tchIsOk {

		if sdata.Id == tdata.Id {
			// 主键一样
			if sdata.Sum == tdata.Sum {
				this.SameRows++
			} else {
				this.DiffRows++
				diffList = append(diffList, sdata.Id)
				if len(diffList) >= 10000 {
					this.DiffRows = len(diffList)
					this.SourceMoreRows = len(SourceMoreDict)
					this.TargetMoreRows = len(TargetMoreDict)
					this.Result = "no"
					this.Error = "DiffRows达到10000行，停止核对"
					slog.Errorf("[%s.%s] DiffRows达到10000行，停止核对", this.DbName, this.TbName)
					//发送停止信号
					stopSource <- struct{}{}
					stopTarget <- struct{}{}
					return
				}
			}
			sdata, schIsOk = <-this.SourceChan
			tdata, tchIsOk = <-this.TargetChan

		} else {
			// 取出的id不一样时，分别到SourceMoreDict TargetMoreDict查找
			// 查找TargetMoreDict
			tv, tok := TargetMoreDict[sdata.Id]
			if tok {
				// 找到值，判断是否相等
				delete(TargetMoreDict, sdata.Id)
				if sdata.Sum == tv {
					this.SameRows++
				} else {
					this.DiffRows++
					diffList = append(diffList, sdata.Id)
					if len(diffList) >= 10000 {
						this.DiffRows = len(diffList)
						this.SourceMoreRows = len(SourceMoreDict)
						this.TargetMoreRows = len(TargetMoreDict)
						this.Result = "no"
						this.Error = "DiffRows达到10000行，停止核对"
						slog.Errorf("[%s.%s] DiffRows达到10000行，停止核对", this.DbName, this.TbName)
						//发送停止信号
						stopSource <- struct{}{}
						stopTarget <- struct{}{}
						return
					}
				}

				sdata, schIsOk = <-this.SourceChan
			} else {
				// 查找SourceMoreDict
				sv, sok := SourceMoreDict[tdata.Id]
				if sok {
					// 找到值，判断是否相等
					delete(SourceMoreDict, tdata.Id)
					if tdata.Sum == sv {
						this.SameRows++
					} else {
						this.DiffRows++
						diffList = append(diffList, tdata.Id)
						if len(diffList) >= 10000 {
							this.DiffRows = len(diffList)
							this.SourceMoreRows = len(SourceMoreDict)
							this.TargetMoreRows = len(TargetMoreDict)
							this.Result = "no"
							this.Error = "DiffRows达到10000行，停止核对"
							slog.Errorf("[%s.%s] DiffRows达到10000行，停止核对", this.DbName, this.TbName)
							//发送停止信号
							stopSource <- struct{}{}
							stopTarget <- struct{}{}
							return
						}
					}

					tdata, tchIsOk = <-this.TargetChan

				} else {
					// 都找不到,把数据存入SourceMoreDict TargetMoreDict
					SourceMoreDict[sdata.Id] = sdata.Sum
					TargetMoreDict[tdata.Id] = tdata.Sum
					if len(SourceMoreDict) >= 10000 {
						this.DiffRows = len(diffList)
						this.SourceMoreRows = len(SourceMoreDict)
						this.TargetMoreRows = len(TargetMoreDict)
						this.Result = "no"
						this.Error = "SourceMoreRows达到10000行，停止核对"
						slog.Errorf("[%s.%s] SourceMoreRows达到10000行，停止核对", this.DbName, this.TbName)
						//发送停止信号
						stopSource <- struct{}{}
						stopTarget <- struct{}{}
						return
					} else if len(TargetMoreDict) >= 10000 {
						this.DiffRows = len(diffList)
						this.SourceMoreRows = len(SourceMoreDict)
						this.TargetMoreRows = len(TargetMoreDict)
						this.Result = "no"
						this.Error = "TargetMoreRows达到10000行，停止核对"
						slog.Errorf("[%s.%s] TargetMoreRows达到10000行，停止核对", this.DbName, this.TbName)
						//发送停止信号
						stopSource <- struct{}{}
						stopTarget <- struct{}{}
						return
					}
					sdata, schIsOk = <-this.SourceChan
					tdata, tchIsOk = <-this.TargetChan
				}
			}
		}
	}

	//不一致行数达到10000行时，以下步骤不执行
	//读取并存入剩余的channel数据
	if schIsOk {
		SourceMoreDict[sdata.Id] = sdata.Sum //把最后一条没核对的数据存入，这条数据会在复核环节再核对
		for sdata := range this.SourceChan {
			SourceMoreDict[sdata.Id] = sdata.Sum
			if len(SourceMoreDict) >= 10000 { //修复内存溢出
				this.DiffRows = len(diffList)
				this.SourceMoreRows = len(SourceMoreDict)
				this.TargetMoreRows = len(TargetMoreDict)
				this.Result = "no"
				this.Error = "SourceMoreRows达到10000行，停止核对"
				slog.Errorf("[%s.%s] SourceMoreRows达到10000行，停止核对", this.DbName, this.TbName)
				//发送停止信号
				stopSource <- struct{}{}
				stopTarget <- struct{}{}
				return
			}
		}
	}
	if tchIsOk {
		TargetMoreDict[tdata.Id] = tdata.Sum //把最后一条没核对的数据存入，这条数据会在复核环节再核对
		for tdata := range this.TargetChan {
			TargetMoreDict[tdata.Id] = tdata.Sum
			if len(TargetMoreDict) >= 10000 { //修复内存溢出
				this.DiffRows = len(diffList)
				this.SourceMoreRows = len(SourceMoreDict)
				this.TargetMoreRows = len(TargetMoreDict)
				this.Result = "no"
				this.Error = "TargetMoreRows达到10000行，停止核对"
				slog.Errorf("[%s.%s] TargetMoreRows达到10000行，停止核对", this.DbName, this.TbName)
				//发送停止信号
				stopSource <- struct{}{}
				stopTarget <- struct{}{}
				return
			}
		}
	}

	slog.Infof("[%s.%s] 完成初核数据", this.DbName, this.TbName)

	//处理结果
	this.DiffRows = len(diffList)
	this.SourceMoreRows = len(SourceMoreDict)
	this.TargetMoreRows = len(TargetMoreDict)
	this.RecheckPassRows = -1
	recheckRows := this.DiffRows + this.SourceMoreRows + this.TargetMoreRows

	if this.Result == "unknown" {
		return //跳过复核
	} else {
		if recheckRows == 0 {
			this.Result = "yes"
		} else {
			this.Result = "no"
		}
	}

	//复核
	if recheckRows > 0 && recheckRows <= this.MaxRecheckRows {
		slog.Infof("[%s.%s] 开始复核数据", this.DbName, this.TbName)
		idtextList := make([]string, 0)
		recheckPassList := make([]string, 0)
		for _, idtext := range diffList {
			idtextList = append(idtextList, idtext)
		}
		for idtext, _ := range SourceMoreDict {
			idtextList = append(idtextList, idtext)
		}
		for idtext, _ := range TargetMoreDict {
			idtextList = append(idtextList, idtext)
		}

		this.RecheckRows(&idtextList, &recheckPassList)
		this.RecheckPassRows = len(recheckPassList)
		slog.Infof("[%s.%s] 复核行数:%d 复核通过行数:%d 复核不通过行数:%d", this.DbName, this.TbName, recheckRows, this.RecheckPassRows, len(idtextList))

		//复核结束
		if recheckRows == this.RecheckPassRows {
			this.Result = "yes"
			this.Error = "初核不通过，复核通过"
		}

		//剔除复核通过的记录
		for _, v := range recheckPassList {
			delete(SourceMoreDict, v)
			delete(TargetMoreDict, v)
		}

		//剔除复核通过的记录
		passIdx := make([]int, 0)
		for i, v := range diffList {
			if util.InArray(recheckPassList, v) {
				passIdx = append(passIdx, i)
			}
		}
		//一次性删除，下标不会移动
		util.RemoveSliceMultiElement(&diffList, passIdx)

	} else if recheckRows > this.MaxRecheckRows {
		slog.Infof("[%s.%s] 不一致行数:%d，大于--max-recheck-rows参数，跳过复核", this.DbName, this.TbName, recheckRows)
	}

	//导出不一致的数据SQL
	if len(diffList) > 0 {
		sqltext := "/* 开启--skip-cols参数时，生成的修复SQL会缺失这些跳过的列 */\n"
		sqltext += fmt.Sprintf("use %s;\n", this.DbGroup.TargetDb)
		for _, idtext := range diffList {
			text, err := this.GetRepairSQL(idtext, 2)
			if err != nil {
				slog.Errorf("[%s.%s] 导出修复SQL报错，%s", this.DbName, this.TbName, err)
			} else {
				sqltext += text
			}
		}
		updateFile := fmt.Sprintf("%s/%s.update.sql", this.DbGroup.Logdir, this.TbName)
		slog.Infof("[%s.%s] 导出修复SQL:%s", this.DbName, this.TbName, updateFile)
		util.WriteFile(updateFile, sqltext)
	}

	if len(SourceMoreDict) > 0 {
		sqltext := "/* 开启--skip-cols参数时，生成的修复SQL会缺失这些跳过的列 */\n"
		sqltext += fmt.Sprintf("use %s;\n", this.DbGroup.TargetDb)
		for idtext, _ := range SourceMoreDict {
			text, err := this.GetRepairSQL(idtext, 1)
			if err != nil {
				slog.Errorf("[%s.%s] 导出修复SQL报错，%s", this.DbName, this.TbName, err)
			} else {
				sqltext += text
			}
		}
		insertFile := fmt.Sprintf("%s/%s.insert.sql", this.DbGroup.Logdir, this.TbName)
		slog.Infof("[%s.%s] 导出修复SQL到:%s", this.DbName, this.TbName, insertFile)
		util.WriteFile(insertFile, sqltext)
	}

	if len(TargetMoreDict) > 0 {
		sqltext := fmt.Sprintf("use %s;\n", this.DbGroup.TargetDb)
		for idtext, _ := range TargetMoreDict {
			text, err := this.GetRepairSQL(idtext, 0)
			if err != nil {
				slog.Errorf("[%s.%s] 导出修复SQL报错，%s", this.DbName, this.TbName, err)
			} else {
				sqltext += text
			}
		}
		deleteFile := fmt.Sprintf("%s/%s.delete.sql", this.DbGroup.Logdir, this.TbName)
		slog.Infof("[%s.%s] 导出修复SQL到:%s", this.DbName, this.TbName, deleteFile)
		util.WriteFile(deleteFile, sqltext)
	}

	this.ExecuteSeconds = int(time.Now().Unix() - ts)
}

func (this *MysqlTable) RecheckOneRow(idtext string) bool {
	//复核一行数据,相同返回true
	var sql, where string
	ColumnsText := strings.Join(util.EncloseStringArray(this.Columns, "`"), ", ")

	//拼接where
	idValues := strings.Split(idtext, ",")
	for i, v := range this.Keys {
		if i > 0 {
			where += fmt.Sprintf(" and `%s`='%s'", v, idValues[i])
		} else {
			where += fmt.Sprintf("`%s`='%s'", v, idValues[i])
		}
	}

	//核对数据
	sql = fmt.Sprintf("select %s from `%s` where %s", ColumnsText, this.TbName, where)
	srow, err := this.DbGroup.SourceDbConn.QueryReturnDict(sql)
	if err != nil {
		slog.Errorf("[%s.%s] 复核不一致的数据，查询Source端报错：%s", this.DbName, this.TbName, err)
		return false
	}
	trow, err := this.DbGroup.TargetDbConn.QueryReturnDict(sql)
	if err != nil {
		slog.Errorf("[%s.%s] 复核不一致的数据，查询Target端报错：%s", this.DbName, this.TbName, err)
		return false
	}
	if len(srow) == 0 && len(trow) == 0 {
		//this.RecheckPassRows++
		slog.Infof("[%s.%s] 两端均无此数据,复核通过 id:[%s]", this.DbName, this.TbName, idtext)
		return true
	} else if len(srow) == 1 && len(srow) == len(trow) {
		if res, str := util.MapIsEqual(srow[0], trow[0]); res {
			//this.RecheckPassRows++
			slog.Infof("[%s.%s] 数据一致,复核通过 id:[%s]", this.DbName, this.TbName, idtext)
			return true
		} else {
			slog.Infof("[%s.%s] 数据不一致,复核不通过 id:[%s] %s", this.DbName, this.TbName, idtext, str)
		}
	} else {
		slog.Infof("[%s.%s] 两端数据行数不一致，复核不通过 id:[%s] rows:[%d] vs [%d]", this.DbName, this.TbName, idtext, len(srow), len(trow))
	}
	return false
}

func (this *MysqlTable) RecheckRows(idtextListP *[]string, recheckPassListP *[]string) {
	//复核一个数组的idtext
	for i := 1; i <= 3; i++ {
		if len(*idtextListP) == 0 {
			break
		}
		if i > 1 { //睡眠10秒
			time.Sleep(time.Second * 10)
		}
		slog.Infof("[%s.%s] 开始进行第%d次复核，复核行数:%d", this.DbName, this.TbName, i, len(*idtextListP))
		for idx := len(*idtextListP) - 1; idx >= 0; idx-- { //从尾部开始删除，从前面删除，下标变化会导致bug
			idtext := (*idtextListP)[idx]
			if this.RecheckOneRow(idtext) {
				util.RemoveSliceElement(idtextListP, idx) //删除元素
				*recheckPassListP = append(*recheckPassListP, idtext)
			}
		}
	}
}

func (this *MysqlTable) GetResults() util.CheckResults {
	//返回核对结果
	return util.CheckResults{
		DbName:          this.DbName,
		TbName:          this.TbName,
		Result:          this.Result,
		Error:           this.Error,
		SourceRows:      this.SourceRows,
		TargetRows:      this.TargetRows,
		SameRows:        this.SameRows,
		DiffRows:        this.DiffRows,
		SourceMoreRows:  this.SourceMoreRows,
		TargetMoreRows:  this.TargetMoreRows,
		MaxRecheckRows:  this.MaxRecheckRows,
		RecheckPassRows: this.RecheckPassRows,
		ExecuteSeconds:  this.ExecuteSeconds,
	}
}
