# checkData(golang)

## Usage scenarios:

1. Data synchronization verification: When using master-slave replication for data backup or data replication, this tool can be used to verify whether the data between the master and slave databases is consistent. If inconsistencies are found, appropriate measures can be taken to repair the data.

2. Data migration verification: When migrating a database to the cloud or another location, this tool can be used to verify whether the data in the original database and the target database is complete. This can help us verify whether there is any data loss or corruption during the migration process.

3. Data warehouse verification: In data warehouse projects, data often flows downstream from upstream. This tool can be used to verify whether the data between upstream and downstream is consistent. This tool supports two modes of verification: full field verification and partial field verification, which can be selected according to actual needs.

4. Data repair: If inconsistencies are detected during data verification, this tool can automatically generate repair SQL statements to fix the problem. This greatly reduces the workload of administrators and can quickly restore data accuracy. At the same time, the automatically generated repair SQL statements can also be used for backup and documentation in the future.

## 使用场景：
1. 数据同步核对
> 当使用主从同步的方式来实现数据备份或数据复制时，我们可以使用这个工具来核对主从数据库之间的数据是否一致。如果发现不一致的情况，就可以采取相应的措施来修复数据。
2. 数据迁移验证
> 当将数据库迁移到云端或其他地方时，我们可以使用这个工具来核对原数据库和目标数据库之间的数据是否完整。这可以帮助我们验证迁移过程中是否有数据丢失或损坏的情况。
3. 数据仓库核对
> 在数据仓库项目中，数据往往会从上游传递到下游，我们可以使用这个工具来核对上游和下游之间的数据是否一致。这个工具支持全部字段核对和部分字段核对两种模式，可以根据实际需要进行选择。
4. 数据修复
> 如果在数据核对过程中发现数据不一致，这个工具可以自动生成修复SQL语句来修复问题。这大大减轻了管理员的工作负担，并可以快速恢复数据的准确性。

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
  主键数据文件保存在：$tablename.diff，修复sql: $tablename.update.sql
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
--max-recheck-times 初核结束后，不一致的数据会进行复核，此参数控制复核次数。
--max-recheck-rows 初核不一致的行数超过这个值，不会进入复核。
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

postgres 报错：pq: function crc32(text) does not exist
方案1：使用slow模式核对
方案2：使用以下sql创建crc32函数
```azure
CREATE OR REPLACE FUNCTION crc32(text_string text) RETURNS bigint AS $$
DECLARE
    tmp bigint;
    i int;
    j int;
    byte_length int;
    binary_string bytea;
BEGIN
    IF text_string = '' THEN
        RETURN 0;
    END IF;

    i = 0;
    tmp = 4294967295;
    byte_length = bit_length(text_string) / 8;
    binary_string = decode(replace(text_string, E'\\\\', E'\\\\\\\\'), 'escape');
    LOOP
        tmp = (tmp # get_byte(binary_string, i))::bigint;
        i = i + 1;
        j = 0;
        LOOP
            tmp = ((tmp >> 1) # (3988292384 * (tmp & 1)))::bigint;
            j = j + 1;
            IF j >= 8 THEN
                EXIT;
            END IF;
        END LOOP;
        IF i >= byte_length THEN
            EXIT;
        END IF;
    END LOOP;
    RETURN (tmp # 4294967295);
END
$$ IMMUTABLE LANGUAGE plpgsql;
```

#### 核对报告样式参考

```
####################################################################################################
核对文件说明
rpt文件: 核对总览信息
csv文件: 核对明细信息
ExecuteSeconds  : 执行时间，包括复核的时间（秒）
SourceRows      : 源表总行数
TargetRows      : 目标表总行数
SameRows        : 数据一致的行数
DiffRows        : 数据不一致的行数，相关数据的主键保存在: ./192.168.1.202_3306/dbms/$table.diff
SourceMoreRows  : 目标端缺失的数据行数，相关数据主键的保存在: ./192.168.1.202_3306/dbms/$table.tloss
TargetMoreRows  : 目标端多出的数据行数，相关数据主键的保存在: ./192.168.1.202_3306/dbms/$table.tmore
RecheckPassRows : 复核通过的行数，-1：表示没有进行复核
########################################## 核对报告 ################################################
计划核对的数据库 : dbms:dbms
SOURCE端的表数   : 59
TARGET端的表数   : 60
需要核对的表数   : 55
数据一致的表数   : 6
数据不一致的表数 : 49
核对失败的表数   : 0
SOURCE端缺失的表 : db_instances_bak20240423, db_instances_bak20241021, mysql_slowlog_history_aliyun, ora_hist_sqlstat_total_bak1108, table_stats_summary_bak1108
TARGET端缺失的表 : db_instances_bak0814, ora_hist_sqlstat_delta_v2, ora_hist_sqlstat_total_bak0914, ora_hist_sqlstat_total_v2
核对失败的表     :
####################################################################################################
```

#### 如何二开接入其他数据库类型：

1. 新数据库对象实现以下接口:

```
type Table interface {
    GetDbName() string
    GetTbName() string
    PreCheck() bool
    PullSourceDataSum(chan<- *model.Data, <-chan struct{})
    PullTargetDataSum(chan<- *model.Data, <-chan struct{})
    Recheck([]string) []string
    GetRepairSQL(string, int) (string, error)
    GetSourceTableCount()
    GetTargetTableCount()
    GetResult() *model.Result
}
```
2. 在checkData.go增加参数配置

