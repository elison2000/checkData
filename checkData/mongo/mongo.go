package mongo

import (
	"checkData/util"
	"context"
	"fmt"
	"github.com/gookit/slog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"sync"
	"time"
)

type CheckData struct {
	Id  string
	Sum uint32
}

type MongoDatabase struct {
	Db               string
	SourceDb         string
	TargetDb         string
	SourceHost       string
	SourcePort       int
	TargetHost       string
	TargetPort       int
	SourceDbConn     *util.MongoDb
	TargetDbConn     *util.MongoDb
	SourceTables     []string
	TargetTables     []string
	ToCheckTables    []string
	SourceMoreTables []string
	TargetMoreTables []string
	SkipTables       []string
	ReportFile       string
	Logdir           string
}

func (this *MongoDatabase) Init(opt util.Options, dbgroup [2]string) {
	this.SourceDb = dbgroup[0]
	this.TargetDb = dbgroup[1]
	this.Db = this.SourceDb
	this.SourceHost = opt.SourceHost
	this.TargetHost = opt.TargetHost
	this.SourcePort = opt.SourcePort
	this.TargetPort = opt.TargetPort
	this.SourceDbConn = &util.MongoDb{Host: this.SourceHost, Port: this.SourcePort, User: opt.User, Password: opt.Password, Database: this.SourceDb}
	this.TargetDbConn = &util.MongoDb{Host: this.TargetHost, Port: this.TargetPort, User: opt.User, Password: opt.Password, Database: this.TargetDb}
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

func (this *MongoDatabase) GetTables() {
	// 获取表名

	//获取源库所有表
	res1, err := this.SourceDbConn.ListCollectionNames(this.SourceDb)
	if err != nil {
		slog.Errorf("[%s:%s] 获取表名失败，%s", this.SourceDb, this.TargetDb, err)
		return
	}

	//剔除系统表
	var filtRes1 []string
	for _, v := range res1 {
		if v != "system.profile" {
			filtRes1 = append(filtRes1, v)
		}
	}
	this.SourceTables = filtRes1

	//获取目标库所有表
	res2, err := this.TargetDbConn.ListCollectionNames(this.TargetDb)
	if err != nil {
		slog.Errorf("[%s:%s] 获取表名失败，%s", this.SourceDb, this.TargetDb, err)
	}

	//剔除系统表
	var filtRes2 []string
	for _, v := range res2 {
		if v != "system.profile" {
			filtRes2 = append(filtRes2, v)
		}
	}
	this.TargetTables = filtRes2

}

func (this *MongoDatabase) GetToCheckTables() {
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

func (this *MongoDatabase) Close() {
	//关闭连接池
	this.SourceDbConn.Close()
	this.TargetDbConn.Close()
	slog.Infof("[%s:%s] 关闭数据库连接池", this.SourceDb, this.TargetDb)
}

type MongoTable struct {
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
	DbGroup         *MongoDatabase
}

func NewMongoTable(tb string, dbg *MongoDatabase, opt util.Options) *MongoTable {
	//MongoTable构造函数

	return &MongoTable{
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

func (this *MongoTable) Init() {
	slog.Infof("[%s.%s] 开始初始化 mode=%s", this.DbName, this.TbName, this.Mode)
	this.SourceChan = make(chan CheckData, 1000)
	this.TargetChan = make(chan CheckData, 1000)
	this.RecheckPassRows = -1
}

func (this *MongoTable) GetName() string {
	//获取表名
	return this.DbName + "." + this.TbName
}

func (this *MongoTable) GetMode() string {
	return this.Mode
}

func (this *MongoTable) Precheck() bool {
	//预检查
	slog.Infof("[%s.%s] 执行预检查", this.DbName, this.TbName)

	//获取列名
	tb := this.DbGroup.SourceDbConn.Tb(this.DbName, this.TbName)
	raw, err := tb.FindOne(context.TODO(), bson.M{}).DecodeBytes()
	if err != nil {
		return false
	}
	id := raw.Lookup("_id").String()
	if id == "" {
		this.Result = "unknown"
		this.Error = "检测_id失败"
		slog.Errorf("[%s.%s] 检测_id失败，跳过核对", this.DbName, this.TbName)
		return false
	} else {
		return true
	}

}

func (this *MongoTable) GetSourceCRC32Data(stopSource, stopTarget chan struct{}) {
	//获取源端数据

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

	slog.Infof("[%s.%s] 开始下载source端数据", this.DbGroup.SourceDb, this.TbName)
	findOptions := options.Find()
	findOptions.SetSort(bson.D{{"_id", 1}})
	cur, err := this.DbGroup.SourceDbConn.Tb(this.DbGroup.SourceDb, this.TbName).Find(context.TODO(), bson.M{}, findOptions)
	if err != nil {
		slog.Infof("[%s.%s] 下载source端数据报错，%s", this.DbGroup.SourceDb, this.TbName, err)
	}

	var raw bson.Raw
	var data CheckData
	for cur.Next(context.TODO()) {
		err := cur.Decode(&raw)
		if err != nil {
			panic(err)
		}
		data.Id = raw.Lookup("_id").String()
		data.Sum = util.CRC32Bytes(raw)
		select {
		case this.SourceChan <- data:
			this.SourceRows++
		case <-stopSource:
			slog.Infof("收到停止信号，结束数据下载[%s.%s]", this.DbGroup.TargetDb, this.TbName)
			return
		}
	}

}

func (this *MongoTable) GetTargetCRC32Data(stopSource, stopTarget chan struct{}) {
	//获取目标端数据

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

	slog.Infof("[%s.%s] 开始下载Target端数据", this.DbGroup.TargetDb, this.TbName)
	findOptions := options.Find()
	findOptions.SetSort(bson.D{{"_id", 1}})
	cur, err := this.DbGroup.TargetDbConn.Tb(this.DbGroup.TargetDb, this.TbName).Find(context.TODO(), bson.M{}, findOptions)
	if err != nil {
		slog.Infof("[%s.%s] 下载Target端数据报错，%s", this.DbGroup.TargetDb, this.TbName, err)
	}

	var raw bson.Raw
	var data CheckData
	for cur.Next(context.TODO()) {
		err := cur.Decode(&raw)
		if err != nil {
			panic(err)
		}
		data.Id = raw.Lookup("_id").String()
		data.Sum = util.CRC32Bytes(raw)
		select {
		case this.TargetChan <- data:
			this.TargetRows++
		case <-stopTarget:
			slog.Infof("收到停止信号，结束数据下载[%s.%s]", this.DbGroup.TargetDb, this.TbName)
			return
		}
	}

}

func (this *MongoTable) GetSourceTableCount(wg *sync.WaitGroup) {
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
	slog.Infof("[%s.%s] 开始计算Source端总行数", this.DbGroup.SourceDb, this.TbName)
	rowcnt, err := this.DbGroup.SourceDbConn.Tb(this.DbGroup.SourceDb, this.TbName).CountDocuments(context.TODO(), bson.M{})
	if err != nil {
		panic(err)
	} else {
		this.SourceRows = int(rowcnt)
	}
}

func (this *MongoTable) GetTargetTableCount(wg *sync.WaitGroup) {
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
	slog.Infof("[%s.%s] 开始计算Target端总行数", this.DbGroup.TargetDb, this.TbName)
	rowcnt, err := this.DbGroup.TargetDbConn.Tb(this.DbGroup.TargetDb, this.TbName).CountDocuments(context.TODO(), bson.M{})
	if err != nil {
		panic(err)
	} else {
		this.TargetRows = int(rowcnt)
	}
}

func (this *MongoTable) CheckTableCount() {
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

func (this *MongoTable) CheckTableDetail() {
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

	//if this.Mode="slow"{}
	go this.GetSourceCRC32Data(stopSource, stopTarget)
	go this.GetTargetCRC32Data(stopSource, stopTarget)

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

	this.ExecuteSeconds = int(time.Now().Unix() - ts)
}

func (this *MongoTable) RecheckOneRow(idtext string) bool {
	//复核一行数据,相同返回true

	//生成filter
	filterStr := fmt.Sprintf(`{"_id" : %s}`, idtext)
	var filter interface{}
	bson.UnmarshalExtJSON([]byte(filterStr), false, &filter)
	tb1 := this.DbGroup.SourceDbConn.Tb(this.DbGroup.SourceDb, this.TbName)
	tb2 := this.DbGroup.TargetDbConn.Tb(this.DbGroup.TargetDb, this.TbName)
	//核对数据
	raw1, err1 := tb1.FindOne(context.TODO(), filter).DecodeBytes()
	raw2, err2 := tb2.FindOne(context.TODO(), filter).DecodeBytes()
	if err1 != nil && err2 != nil {
		if err1.Error() == "mongo: no documents in result" && err2.Error() == "mongo: no documents in result" {
			slog.Infof("[%s.%s] %s 两端都没有此数据,复核通过", this.DbGroup.SourceDb, this.TbName, filterStr)
			return true
		} else {
			slog.Errorf("[%s.%s] %s 复核Source端数据报错：%s", this.DbGroup.SourceDb, this.TbName, filterStr, err1)
			slog.Errorf("[%s.%s] %s 复核Target端数据报错：%s", this.DbGroup.TargetDb, this.TbName, filterStr, err2)
			return false
		}
	}
	if err1 != nil {
		slog.Errorf("[%s.%s] %s 复核Source端数据报错：%s", this.DbGroup.SourceDb, this.TbName, filterStr, err1)
		return false
	}

	if err2 != nil {
		slog.Errorf("[%s.%s] %s 复核Target端数据报错：%s", this.DbGroup.TargetDb, this.TbName, filterStr, err2)
		return false
	}

	sum1 := util.CRC32Bytes(raw1)
	sum2 := util.CRC32Bytes(raw2)

	if sum1 == sum2 {
		slog.Infof("[%s.%s] %s 两端数据一致,复核通过", this.DbGroup.SourceDb, this.TbName, filterStr)
		return true
	} else {
		slog.Infof("[%s.%s] %s 两端数据不一致，复核不通过", this.DbGroup.SourceDb, this.TbName, filterStr)
	}
	return false
}

func (this *MongoTable) RecheckRows(idtextListP *[]string, recheckPassListP *[]string) {
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

func (this *MongoTable) GetResults() util.CheckResults {
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
