# mysql-sequence-generator
A mysql-based lock-free id generator using last_insert_id() function.

```sql
create table t_sequence(
  name  varchar(100) primary key             comment '序列名称',
  code  varchar(5) not null default '00000'  comment '序列编号（嵌入最终序列中）',
  value bigint     not null default 0        comment '当前值',
  min   bigint     not null default 0        comment '最小值',
  max   bigint     not null default 99999999 comment '最大值，当 value 达到最大值时重新从 min 开始',
  step  int        not null default 1000     comment '步长，每次取序列时缓存多少，步长越大，数据库访问频率越低'
);

INSERT into t_sequence (name, code, max) values ('seq1', '886', 99999999);
INSERT into t_sequence (name, code, max) values ('seq2', '887', 99999999);
INSERT into t_sequence (name, code, max) values ('seq3', '888', 99999999);
```

基于 MySQL 的 `last_insert_id()` 函数而写的序列生成工具。

效率很高，没有数据库事务。

**整个项目只有一个类，且无任何依赖关系，可以直接拷贝到任何项目中使用。**

### 一、使用方法

1. 创建 `MysqlSequenceGenerator` 对象 `MysqlSequenceGenerator generator = new MysqlSequenceGenerator(dataSource)`
1. 获得一个 long 类型的序列：`long id = generator.nextLong("seq1")`
1. 获得一个带年月日和编号的字符串序列：`String id = generator.nextSequence("seq1")` 

使用的示例详见单元测试。

#### 字符串序列的格式

字符串序列的格式为 `yyyyMMdd[code][sequence]`，其中 `[code]` 为 code 字段的值，`[sequence]` 的长度与 max 
字段的值长度相同。例如某个序列，code 字段值为 `888`，max 字段值为 `999999`，那么生成的字符串序列可能为 `"20191231888000001"`。

### 二、兼容 Spring 数据库事务

如果要在 Spring Boot 项目中使用并兼容 Spring 事务，请参考 
`com.hyd.mysqlsequencegenerator.MysqlSequenceGeneratorApplication` 源码示例。

### 三、性能测试

- 10 线程，序列步长  1000，生成 1,000,000 个 ID，耗时 34530 ms
- 10 线程，序列步长 10000，生成 1,000,000 个 ID，耗时  3278 ms

数据库运行在**普通机械硬盘**上，瓶颈都在磁盘 I/O。
