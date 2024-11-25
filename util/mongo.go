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

func (self *MongoDb) Init() (err error) {
    dsn := fmt.Sprintf("mongodb://%s:%s@%s:%d/?connect=direct;authSource=admin", self.User, self.Password, self.Host, self.Port)
    clientOptions := options.Client().ApplyURI(dsn)
    client, err := mongo.Connect(context.TODO(), clientOptions)
    if err != nil {
        return
    }
    self.Client = client
    return
}

func (self *MongoDb) Close() {
    //关闭连接池
    err := self.Client.Disconnect(context.TODO())
    if err != nil {
        slog.Errorf("关闭连接池失败，%s", err)
    }
}

func (self *MongoDb) ListDatabaseNames() ([]string, error) {
    //查看数据库
    res, err := self.Client.ListDatabaseNames(context.TODO(), bson.M{})
    //slog.Info(res, err)
    return res, err
}

func (self *MongoDb) ListCollectionNames(dbname string) ([]string, error) {
    //查看表
    res, err := self.Client.Database(dbname).ListCollectionNames(context.TODO(), bson.M{})
    //slog.Info(res, err)
    return res, err
}

func (self *MongoDb) Db(dbname string) *mongo.Database {
    return self.Client.Database(dbname)
}

func (self *MongoDb) Tb(dbname, tbname string) *mongo.Collection {
    return self.Client.Database(dbname).Collection(tbname)
}

func (self *MongoDb) FindAll(dbname, tbname string) {
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
        //idbytes := raw.Lookup("_id")
        //fmt.Println(idbytes)
        //idbytes := raw.Lookup("_id").Value
        //idhex := hex.EncodeToString(idbytes)
        //fmt.Println(idbytes, idhex)

        time.Sleep(time.Second)
    }

}
