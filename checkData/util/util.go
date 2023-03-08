package util

import (
	"encoding/hex"
	"fmt"
	"github.com/gookit/slog"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"hash/crc32"
	"os"
	"sort"
	"time"
)

type CheckResults struct {
	DbName          string
	TbName          string
	Result          string
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
}

//func RemoveSliceElement[T any](list []T, index int) []T {
//	//删除slice某个元素
//	if index < 0 || index >= len(list) {
//		return list
//	}
//	//第index开始，后面的所有元素左移一位
//	copy(list[index:], list[index+1:])
//	//把最后一个元素剔除
//	return list[:len(list)-1]
//}

func RemoveSliceElement[T any](list *[]T, index int) {
	//删除slice某个元素
	*list = append((*list)[:index], (*list)[index+1:]...)
}

func RemoveSliceMultiElement[T any](list *[]T, indexes []int) {
	//删除slice某几个元素
	sort.Ints(indexes)
	for i := len(indexes) - 1; i >= 0; i-- {
		*list = append((*list)[:indexes[i]], (*list)[indexes[i]+1:]...)
	}
}

func MapIsEqual(s, t map[string]string) (bool, string) {
	//对比2个map是否相等
	var res string
	if len(s) != len(t) {
		res = fmt.Sprintf("key number:[%d] vs [%d]", len(s), len(t))
		return false, res
	}
	for sk, sv := range s {
		tv := t[sk]
		if sv != tv {
			res = fmt.Sprintf("key:%s values:[%s] vs [%s]", sk, sv, tv)
			return false, res
		}
	}
	return true, res
}

//func MapIsEqual(s, t map[string]string) (bool, string) {
//	//对比2个map是否相等
//	sstr := fmt.Sprintf("%+v", s)
//	tstr := fmt.Sprintf("%+v", t)
//	ssum := CRC32Bytes([]byte(sstr))
//	tsum := CRC32Bytes([]byte(tstr))
//
//	if ssum == tsum {
//		return true, ""
//
//	} else {
//		return false, ""
//	}
//
//}

func EncloseStringArray(strlist []string, mark string) []string {
	//使用符号包围字符串
	var quetaList = make([]string, len(strlist))
	for i, v := range strlist {
		quetaList[i] = mark + v + mark
	}
	return quetaList
}

func CRC32(str string) uint32 {
	//计算CRC32编码
	return crc32.ChecksumIEEE([]byte(str))
}

func CRC32Bytes(buf []byte) uint32 {
	//计算CRC32编码
	return crc32.ChecksumIEEE(buf)
}

func ByteToHex(b []byte) string {
	return hex.EncodeToString(b)
}

func ObjectIDToHex(oid primitive.ObjectID) string {
	return oid.Hex()
}

func ObjectIDFromHex(str string) primitive.ObjectID {
	// filter := bson.M{"_id": ObjectIDFromHex(str)}
	oid, _ := primitive.ObjectIDFromHex(str)
	return oid
}

func TimeCost() func(str string) {
	//计算耗时
	bts := time.Now().Unix()
	return func(str string) {
		ts := time.Now().Unix() - bts
		slog.Infof("%s，耗时%ds", str, ts)
	}
}

func InArray(str_array []string, target string) bool {
	sort.Strings(str_array)
	index := sort.SearchStrings(str_array, target)
	if index < len(str_array) && str_array[index] == target {
		return true
	}
	return false
}

func WriteFile(filename string, text string) {
	//写入文件
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0664)
	if err != nil {
		slog.Fatal(err)
	}
	defer f.Close()
	f.WriteString(text)
}

func WriteFileTail(filename string, text string) {
	//写入文件尾部（追加）
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModeAppend|os.ModePerm)
	if err != nil {
		slog.Fatal(err)
	}
	defer f.Close()
	f.WriteString(text)
}

func File(filename string) (*os.File, error) {
	//获取文件句柄
	//		f.WriteString("test")
	//		f.Close()
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0664)
	return f, err
}
