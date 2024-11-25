package util

import (
    "encoding/hex"
    "fmt"
    "github.com/gookit/slog"
    "go.mongodb.org/mongo-driver/bson/primitive"
    "hash/crc32"
    "os"
    "path/filepath"
    "sort"
    "time"
)

func EnterWorkDir() {
    fullpath, err := os.Executable()
    if err != nil {
        panic(err)
    }
    dir, _ := filepath.Split(fullpath)
    err = os.Chdir(dir)
    if err != nil {
        panic(err)
    }
    currentDir, _ := os.Getwd()
    fmt.Printf("当前目录为: %s\n", currentDir)
}

func Mkdir(dirName string) error {
    if _, err := os.Stat(dirName); os.IsNotExist(err) {
        err := os.MkdirAll(dirName, 0775) //目录不存在，创建目录
        if err != nil {
            //return err
            return fmt.Errorf("mkdir(%s) -> %w", dirName, err)
        }
    }
    return nil
}

func RemoveSliceElement[T comparable](list *[]T, index int) {
    //删除slice某个元素
    *list = append((*list)[:index], (*list)[index+1:]...)
}

func RemoveSliceMultiElement[T comparable](list *[]T, toDel *[]T) {

    indexes := make([]int, 0)
    for i, v := range *list {
        if InSlice(v, *toDel) {
            indexes = append(indexes, i)
        }
    }

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

func EncloseStringArray(strlist []string, mark string) []string {
    //使用符号包围字符串
    var quetaList = make([]string, len(strlist))
    for i, v := range strlist {
        quetaList[i] = mark + v + mark
    }
    return quetaList
}

func CRC32(str *string) uint32 {
    //计算CRC32编码
    return crc32.ChecksumIEEE([]byte(*str))
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

//func TimeCost() func(str string) {
//    //计算耗时
//    bts := time.Now().Unix()
//    return func(str string) {
//        ts := time.Now().Unix() - bts
//        slog.Infof("%s，耗时%ds", str, ts)
//    }
//}

func TimeCost() func(str string) {
    //计算耗时
    bts := time.Now()
    return func(str string) {
        second := int(time.Since(bts).Seconds())
        slog.Infof("%s，耗时%ds", str, second)
    }
}

func InSlice[T comparable](target T, list []T) bool {
    for i, _ := range list {
        if target == list[i] {
            return true
        }
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
