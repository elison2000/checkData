package mongo

import (
	"checkData/model"
	"checkData/util"
	"context"
	"fmt"
	"github.com/gookit/slog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Table struct {
	DbName      string
	TbName      string
	Mode        string //fast,slow,count
	Keys        []string
	Columns     []string
	Where       string
	SkipColumns []string
	DbGroup     *Database
	Result      *model.Result
}

func (self *Table) GetDbName() string {
	return self.DbName
}

func (self *Table) GetTbName() string {
	return self.TbName
}

func (self *Table) PreCheck() bool {
	//预检查
	slog.Infof("[%s.%s] 执行预检查", self.DbName, self.TbName)

	if self.Mode == "count" {
		return true
	}

	//获取列名
	tb := self.DbGroup.SourceDbConn.Tb(self.DbName, self.TbName)
	raw, err := tb.FindOne(context.TODO(), bson.M{}).DecodeBytes()
	if err != nil {
		self.Result.Status = -1
		self.Result.Message = fmt.Sprintf("[%s.%s] 获取数据失败:%s", self.DbName, self.TbName, err)
		slog.Errorf("[%s.%s] 获取数据失败:%s", self.DbName, self.TbName, err)
		return false
	}
	id := raw.Lookup("_id").String()
	if id == "" {
		self.Result.Status = -1
		self.Result.Message = "检测_id失败"
		slog.Errorf("[%s.%s] 检测_id失败", self.DbName, self.TbName)
		return false
	} else {
		return true
	}

}

func (self *Table) pullSourceDataSumSlow(dataCh chan<- *model.Data, doneCh <-chan struct{}) error {
	//获取源端数据

	slog.Infof("[%s.%s] 开始下载source端数据", self.DbGroup.SourceDb, self.TbName)
	findOptions := options.Find()
	findOptions.SetSort(bson.D{{"_id", 1}})
	cur, err := self.DbGroup.SourceDbConn.Tb(self.DbGroup.SourceDb, self.TbName).Find(context.TODO(), bson.M{}, findOptions)
	if err != nil {
		slog.Infof("[%s.%s] 下载source端数据报错，%s", self.DbGroup.SourceDb, self.TbName, err)
	}

	var raw bson.Raw
	for cur.Next(context.TODO()) {
		err := cur.Decode(&raw)
		if err != nil {
			return fmt.Errorf("pullSourceDataSumSlow:Decode -> %w", err)
		}
		data := model.Data{raw.Lookup("_id").String(), util.CRC32Bytes(raw)}
		select {
		case dataCh <- &data:
			self.Result.SourceRows++
		case <-doneCh:
			slog.Infof("收到停止信号，结束数据下载[%s.%s]", self.DbGroup.TargetDb, self.TbName)
			return nil
		}
	}
	return nil

}

func (self *Table) pullTargetDataSumSlow(dataCh chan<- *model.Data, doneCh <-chan struct{}) error {
	//获取目标端数据

	slog.Infof("[%s.%s] 开始下载Target端数据", self.DbGroup.TargetDb, self.TbName)
	findOptions := options.Find()
	findOptions.SetSort(bson.D{{"_id", 1}})
	cur, err := self.DbGroup.TargetDbConn.Tb(self.DbGroup.TargetDb, self.TbName).Find(context.TODO(), bson.M{}, findOptions)
	if err != nil {
		slog.Infof("[%s.%s] 下载Target端数据报错，%s", self.DbGroup.TargetDb, self.TbName, err)
	}

	var raw bson.Raw
	for cur.Next(context.TODO()) {
		err := cur.Decode(&raw)
		if err != nil {
			return fmt.Errorf("pullTargetDataSumSlow:Decode -> %w", err)
		}
		data := model.Data{raw.Lookup("_id").String(), util.CRC32Bytes(raw)}
		select {
		case dataCh <- &data:
			self.Result.TargetRows++
		case <-doneCh:
			slog.Infof("收到停止信号，结束数据下载[%s.%s]", self.DbGroup.TargetDb, self.TbName)
			return nil
		}
	}
	return nil
}

func (self *Table) PullSourceDataSum(dataCh chan<- *model.Data, doneCh <-chan struct{}) {
	defer util.TimeCost()(fmt.Sprintf("[%s.%s] Source端数据下载完成", self.DbName, self.TbName))
	defer close(dataCh)

	slog.Infof("[%s.%s] 开始下载Source端数据", self.DbGroup.SourceDb, self.TbName)
	var err error
	self.pullSourceDataSumSlow(dataCh, doneCh)
	if err != nil {
		self.Result.Status = -1
		self.Result.Message = err.Error()
		slog.Error(self.Result.Message)
		return
	}
}

func (self *Table) PullTargetDataSum(dataCh chan<- *model.Data, doneCh <-chan struct{}) {
	defer util.TimeCost()(fmt.Sprintf("[%s.%s] Target端数据下载完成", self.DbName, self.TbName))
	defer close(dataCh)

	slog.Infof("[%s.%s] 开始下载Target端数据", self.DbGroup.TargetDb, self.TbName)
	var err error
	self.pullTargetDataSumSlow(dataCh, doneCh)
	if err != nil {
		self.Result.Status = -1
		self.Result.Message = err.Error()
		slog.Error(self.Result.Message)
		return
	}
}

func (self *Table) Recheck(idTextList []string) (passList []string) {
	for _, idText := range idTextList {
		if self.recheckOne(idText) {
			passList = append(passList, idText)
		}
	}
	return passList
}

func (self *Table) GetRepairSQL(string, int) (string, error) {
	return "", fmt.Errorf("GetRepairSQL:Unsupported")
}

func (self *Table) GetSourceTableCount() {
	// 返回表总行数
	defer util.TimeCost()(fmt.Sprintf("[%s.%s] Source端总行数统计完成", self.DbGroup.SourceDb, self.TbName))

	slog.Infof("[%s.%s] 开始计算Source端总行数", self.DbGroup.SourceDb, self.TbName)
	cnt, err := self.DbGroup.SourceDbConn.Tb(self.DbGroup.SourceDb, self.TbName).CountDocuments(context.TODO(), bson.M{})
	if err != nil {
		self.Result.Status = -1
		self.Result.Message = fmt.Errorf("GetSourceTableCount:CountDocuments -> %w", err).Error()
		slog.Error(err)
		return
	}
	self.Result.SourceRows = int(cnt)
}

func (self *Table) GetTargetTableCount() {
	// 返回表总行数
	defer util.TimeCost()(fmt.Sprintf("[%s.%s] Target端总行数统计完成", self.DbGroup.TargetDb, self.TbName))

	slog.Infof("[%s.%s] 开始计算Target端总行数", self.DbGroup.TargetDb, self.TbName)
	cnt, err := self.DbGroup.TargetDbConn.Tb(self.DbGroup.TargetDb, self.TbName).CountDocuments(context.TODO(), bson.M{})
	if err != nil {
		self.Result.Status = -1
		self.Result.Message = fmt.Errorf("GetTargetTableCount:CountDocuments -> %w", err).Error()
		slog.Error(err)
		return
	}
	self.Result.TargetRows = int(cnt)
}

func (self *Table) recheckOne(idText string) bool {
	//复核一行数据,相同返回true

	//生成filter
	filterStr := fmt.Sprintf(`{"_id" : %s}`, idText)
	var filter interface{}
	bson.UnmarshalExtJSON([]byte(filterStr), false, &filter)
	tb1 := self.DbGroup.SourceDbConn.Tb(self.DbGroup.SourceDb, self.TbName)
	tb2 := self.DbGroup.TargetDbConn.Tb(self.DbGroup.TargetDb, self.TbName)
	//核对数据
	raw1, err1 := tb1.FindOne(context.TODO(), filter).DecodeBytes()
	raw2, err2 := tb2.FindOne(context.TODO(), filter).DecodeBytes()
	if err1 != nil && err2 != nil {
		if err1.Error() == "mongo: no documents in result" && err2.Error() == "mongo: no documents in result" {
			slog.Infof("[%s.%s] %s 两端都没有此数据,复核通过", self.DbGroup.SourceDb, self.TbName, filterStr)
			return true
		} else {
			slog.Errorf("[%s.%s] %s 复核Source端数据报错：%s", self.DbGroup.SourceDb, self.TbName, filterStr, err1)
			slog.Errorf("[%s.%s] %s 复核Target端数据报错：%s", self.DbGroup.TargetDb, self.TbName, filterStr, err2)
			return false
		}
	}
	if err1 != nil {
		slog.Errorf("[%s.%s] %s 复核Source端数据报错：%s", self.DbGroup.SourceDb, self.TbName, filterStr, err1)
		return false
	}

	if err2 != nil {
		slog.Errorf("[%s.%s] %s 复核Target端数据报错：%s", self.DbGroup.TargetDb, self.TbName, filterStr, err2)
		return false
	}

	sum1 := util.CRC32Bytes(raw1)
	sum2 := util.CRC32Bytes(raw2)

	if sum1 == sum2 {
		slog.Infof("[%s.%s] %s 两端数据一致,复核通过", self.DbGroup.SourceDb, self.TbName, filterStr)
		return true
	} else {
		slog.Infof("[%s.%s] %s 两端数据不一致，复核不通过", self.DbGroup.SourceDb, self.TbName, filterStr)
	}
	return false
}

func (self *Table) GetResult() *model.Result {
	return self.Result
}
