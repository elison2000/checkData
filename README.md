# checkData(golang版本)
## 使用场景：
1. 核对主从同步的数据库是否一致。
2. 核对自建和RDS之间的数据库是否完整，我们用来验证上云迁移的数据完整性。
3. 用于核对数仓项目中的上游和下游的数据是否一致，支持全部字段核对和部分字段核对两种模式。
4. 某些不重要的数据，只关心总行数和核对速度，不关心值是否一致，可以只核对总行数（不核对明细数据）。

## 优点：
1. 支持多种数据库版本：mysql(tidb/doris),mongo,postgresql。
2. 相比pt-table-checksum，pt-table-checksum是侵入式工具: 在主库执行crc32计算操作，这个计算sql以statement模式存入binlog，binlog同步到备库，在备库上replay时再次计算，最后对比2端的crc32计算结果。
   这种操作，效率低，会导致备库延时，如果有多个备库，其他不核对的备库也受到影响。本工具不存在这个问题。
3. 支持sql层同协议的数据库对比，比如mysql和tidb/doris对比。
4. 性能高，支持自定义并发数。
5. 核对账号只需要查询权限，不会对数据库进行DML操作。
6. 支持源端和目标端不同库名的核对方式(表名必须一致)，比如，在同一个实例有2个库，分别是db1、db01，对比他们之间的差异：./checkData -S 127.1:3306 -T 127.1:3306 -d db1:db01
7. 差异数据自动生成修复的sql脚本，核对结果分3类，不一致的数据和修复sql分类存放，这样可以自由选择修复哪个类型的数据。

* 数据不同(两端都找到同一个主键值，但是其他列的值不一样)  
主键数据文件保存在：$tablename.diff，修复sql: $tablename.replace.sql
* 目标端不存在该主键值的记录  
主键数据文件保存在：$tablename.tloss，修复sql:$tablename.insert.sql
* 目标端多出的记录（源端不存在该主键值的记录）  
主键数据文件保存在：$tablename.tmore，修复sql:$tablename.delete.sql



## 原理：
本程序使用crc32算法，计算两端每一条记录的所有列的数据的校验和，得到源端和目标端2个{id,crc32sum}的数据集合，然后对比2个集合的差异。得到的差异数据，会进行3次复核，复核不通过的记录算作不一致的数据。当然为了提高核对效率，做了一些优化。  
计算crc32有两种方式：  
* 本地计算  
在本程序里计算crc32，网络延时、表存在大字段等因素对性能影响大，兼容性高，对应的是--mode=slow。  
* 远端计算  
在数据库端计算crc32，需要数据库支持crc32函数，速度快，兼容性低，对应的是--mode=fast(默认模式)。  

## 特别说明：

1. 在初核阶段，核到的数据超过10000条数据不一致，终止核对。  
可通过抽样分析这10000条记录，找到不一致原因，然后自行修复。修复后运行本程序复核。
2. 使用skipcols跳过字段，在修复的sql脚本中，不包括这些字段的信息。  
使用了skipcols参数，使用程序生成的修复语句，会缺失这些列的数据，执行时，会导致这些列的值为空（或默认值）。（这设计目的是为了兼容我们公司ODS仓库，ODS字段和业务系统有差异。）
3. pgsql我们公司使用场景少，可能存在bug
4. 源端和目标端使用核对的用户和密码必须一样，需要查询权限（包括查看表结构和数据等）。
5. 在mode=fast下，时间主要消耗在初核阶段，核对速度取决于db端sql的速度和网络延时，如果复核速度过慢，可以通过设置--max-recheck-rows=0参数，跳过复核环节。

## 使用方法：
下载程序checkData，并授权：chmod +x checkData  
目前支持5个子命令
```
./checkData help      查看帮助
./checkData version   查看版本
./checkData mysql [command options]    核对支持mysql协议数据库
./checkData mongo [command options]    核对mongo数据库
./checkData pgsql [command options]    核对postgresql数据库
```

### 部分选项说明：
#### 查看mysql子命令的帮助信息
```
./checkData help mysql  
./checkData mongo -S 192.168.1.201:28017 -T 192.168.1.202:28017 -u dba_ro -p abc123 -d crmdb
./checkData pgsql -S 192.168.1.201:5432  -T 192.168.1.202:5432  -u dba_ro -p abc123 -d finance
```
#### 核对模式
```
--mode  mode:[fast|slow|count]
fast: 快速模式，数据库必须支持函数crc32()
slow: 兼容模式, 核对mysql和doris/tidb之间的数据
count: 只对比总行数，不对比数据差异
```
#### 其他参数
```
--db 可同时指定多个数据库，如：db1,db2，支持两端库名不同的数据库，如：db1:db01,db2:db02（db1和db01核对，db2和db02核对，灵活组合即可核对同一个实例下的2个不同的库）
--tables 可同时指定多个表，不指定即是全库核对
--Where 可指定条件，核对部分数据，比如核对大表,可利用“on update current_timestamp”的字段 eq: update_time<curdate()。
这个灵活利用，可缩短变更时间。比如做数据库迁移变更，如果需要核对数据。先提前把存量的数据核对完，再多次使用条件进行增量核对，直到变更时，只需要再核对最后一次增量核对到现在产生的数据。
--keys 默认使用主键核对，如果没有主键可以使用该参数指定一个或多个列作为核对的键。这个键必须唯一，不唯一会导致核对结果不准确。
--skipcols  跳过不需要的列，多用于核对 业务数据库和ODS仓库之间的数据。
--max-recheck-rows 由于主库和备库有同步延时，可能会导致检测出来的差异数据实际是一致的，因此第一次检测出的数据差异，每隔10秒会进行复核3次。
--parallel  并行，默认为2，表示同时核对2个表。并行是针对多表的，只核对一个表无需开启这个参数（单个表程序已自动开启2个协程同时下载源端和目标端的数据）。
```

#### 常见问题
核对postgresql报错：permission denied for schema sp_oa  
>核对账号需要正确授权，该账号必须拥有该database下的所有schema的usage和select权限，执行以下语句生成授权SQL：

```
create role dba_ro with login password 'mypassword';
select 'grant usage on schema ' || nspname || ' to dba_ro;' sqltext from pg_namespace where nspname not like 'pg_%';
select 'grant select on all tables in schema ' || nspname || ' to dba_ro;' sqltext from pg_namespace where nspname not like 'pg_%';
```


#### 核对报告样式参考

```
########################################## 核对报告 ################################################
计划核对的数据库 : test:test1
SOURCE端的表数   : 7
TARGET端的表数   : 5
两端共有的表数   : 5
数据一致的表数   : 2
数据不一致的表数 : 3
核对失败的表数   : 0
SOURCE端缺失的表 : 
TARGET端缺失的表 : users2, users
核对失败的表     : 
####################################################################################################
核对结果说明
Result          : yes-数据一致，no-数据不一致，unknown-未知（核对失败）
ExecuteSeconds  : 执行时间，包括复核的时间（秒）
SourceRows      : 源表总行数
TargetRows      : 目标表总行数
SameRows        : 数据一致的行数
DiffRows        : 数据不一致的行数，相关数据的主键保存在: ./192.168.1.203_28017/test/$table.diff
SourceMoreRows  : 目标端缺失的数据行数，相关数据主键的保存在: ./192.168.1.203_28017/test/$table.tloss
TargetMoreRows  : 目标端多出的数据行数，相关数据主键的保存在: ./192.168.1.203_28017/test/$table.tmore
RecheckPassRows : 复核通过的行数，-1：表示没有进行复核
####################################################################################################
TableName, Result, ExecuteSeconds, SourceRows, TargetRows, SameRows, DiffRows, SourceMoreRows, TargetMoreRows, RecheckPassRows, Error
t5, no, 0,65, 64, 0, 0, 0, 0, -1, 
t2, no, 0,2, 3, 0, 0, 0, 0, -1, 
t1, no, 1,341998, 341999, 0, 0, 0, 0, -1, 
t3, yes, 0,244, 244, 0, 0, 0, 0, -1, 
t4, yes, 0,24476, 24476, 0, 0, 0, 0, -1, 
```
