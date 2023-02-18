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

type MongoDb struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Client   *mongo.Client
}

func (this *MongoDb) Init() {
	//初始化
	//MongoDb{Host: "192.168.1.203", Port: 28017, User: "root", Password: "password#ok", Database: "admin"}
	dsn := fmt.Sprintf("mongodb://%s:%s@%s:%d/?connect=direct;authSource=admin", this.User, this.Password, this.Host, this.Port)
	clientOptions := options.Client().ApplyURI(dsn)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		slog.Errorf("启动连接池失败，%s", err)
		return
	}
	this.Client = client
}

func (this *MongoDb) Close() {
	//关闭连接池
	err := this.Client.Disconnect(context.TODO())
	if err != nil {
		slog.Errorf("关闭连接池失败，%s", err)
	}
}

func (this *MongoDb) ListDatabaseNames() ([]string, error) {
	//查看数据库
	res, err := this.Client.ListDatabaseNames(context.TODO(), bson.M{})
	//slog.Info(res, err)
	return res, err
}

func (this *MongoDb) ListCollectionNames(dbname string) ([]string, error) {
	//查看表
	res, err := this.Client.Database(dbname).ListCollectionNames(context.TODO(), bson.M{})
	//slog.Info(res, err)
	return res, err
}

func (this *MongoDb) Db(dbname string) *mongo.Database {
	return this.Client.Database(dbname)
}

func (this *MongoDb) Tb(dbname, tbname string) *mongo.Collection {
	return this.Client.Database(dbname).Collection(tbname)
}

func (this *MongoDb) FindAll(dbname, tbname string) {
	//查询数据
	tb := this.Tb(dbname, tbname)
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
		//idbytes := raw.Lookup("_id")
		//fmt.Println(idbytes)
		//idbytes := raw.Lookup("_id").Value
		//idhex := hex.EncodeToString(idbytes)
		//fmt.Println(idbytes, idhex)

		time.Sleep(time.Second)
	}

}
