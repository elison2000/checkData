package util

import (
	"context"
	"fmt"
	"github.com/gookit/slog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type MongoDB struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Client   *mongo.Client
}

func (self *MongoDB) Init() (err error) {
	dsn := fmt.Sprintf("mongodb://%s:%s@%s:%d/?connect=direct;authSource=admin", self.User, self.Password, self.Host, self.Port)
	clientOptions := options.Client().ApplyURI(dsn)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return
	}
	self.Client = client
	return
}

func (self *MongoDB) Close() {
	//关闭连接池
	err := self.Client.Disconnect(context.TODO())
	if err != nil {
		slog.Errorf("关闭连接池失败，%s", err)
	}
}

func (self *MongoDB) ListDatabaseNames() ([]string, error) {
	//查看数据库
	res, err := self.Client.ListDatabaseNames(context.TODO(), bson.M{})
	//slog.Info(res, err)
	return res, err
}

func (self *MongoDB) ListCollectionNames(dbname string) ([]string, error) {
	//查看表
	res, err := self.Client.Database(dbname).ListCollectionNames(context.TODO(), bson.M{})
	//slog.Info(res, err)
	return res, err
}

func (self *MongoDB) Db(dbname string) *mongo.Database {
	return self.Client.Database(dbname)
}

func (self *MongoDB) Tb(dbname, tbname string) *mongo.Collection {
	return self.Client.Database(dbname).Collection(tbname)
}

func (self *MongoDB) FindAll(dbname, tbname string) {
	//查询数据
	tb := self.Tb(dbname, tbname)
	cur, err := tb.Find(context.TODO(), bson.M{})
	if err != nil {
		slog.Errorf("[%s.%s] 查询报错")
	}
	for cur.Next(context.TODO()) {
		var raw bson.Raw
		err := cur.Decode(&raw)
		if err != nil {
			slog.Errorf("[%s.%s] 查询报错")
		}
		time.Sleep(time.Second)
	}

}
