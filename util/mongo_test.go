package util

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
)

func TestMongoDb_ListDatabaseNames(t *testing.T) {
	conn := &MongoDb{Host: "192.168.1.203", Port: 28017, User: "root", Password: "password#ok", Database: "admin"}
	conn.Init()
	res, err := conn.ListDatabaseNames()
	if err != nil {
		panic(err)
	} else {
		fmt.Println(res[:5])
	}
}

func TestMongoDb_ListCollectionNames(t *testing.T) {
	conn := &MongoDb{Host: "192.168.1.203", Port: 28017, User: "root", Password: "password#ok", Database: "admin"}
	conn.Init()
	res, err := conn.ListCollectionNames("sp_product")
	if err != nil {
		panic(err)
	} else {
		fmt.Println(res[:10])
	}
}

func TestMongoDb_Tb(t *testing.T) {
	conn := &MongoDb{Host: "192.168.1.203", Port: 28017, User: "root", Password: "password#ok", Database: "admin"}
	conn.Init()

	tb := conn.Tb("test1", "t2")
	filterStr := `{"_id" : {"$oid":"63ea0717fe8086feeb4ec456"}}`
	var filter interface{}
	bson.UnmarshalExtJSON([]byte(filterStr), false, &filter)

	findOptions := options.Find()
	findOptions.SetSort(bson.D{{"_id", 1}})

	cur, _ := tb.Find(context.TODO(), bson.D{}, findOptions)
	for cur.Next(context.TODO()) {
		var raw bson.Raw
		cur.Decode(&raw)
		fmt.Println("data: ", raw)
	}
}
