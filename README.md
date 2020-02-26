# mysql-sequence-generator
A mysql-based lock-free id generator using last_insert_id() function.

标准的序列表，但也可兼容字段稍微不同的表，具体见后面说明。

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

### 一、创建方法（表的兼容性）

MysqlSequenceGenerator 不要求表一定要是标准的样子，但要求表中至少要有下面五个字段：

- 序列名称
- 当前值
- 最小值
- 最大值
- 步长

每个字段可以自定义名字，而且标准表中的 `code` 字段是可选的。下面是一个创建 `MysqlSequenceGenerator` 对象的例子：

```java
MysqlSequenceGenerator idGenerator = new MysqlSequenceGenerator(
    dataSource::getConnection, // 获得 Connection 的方式
    Connection::close,         // 关闭 Connection 的方式
    "t_sequence",              // 实际的序列表名称
    false,                     // 是否异步获取新的序列号。提前异步获取新的序列号可提升性能
    Arrays.asList(             // 自定义字段名称
        ColumnInfo.customName(Column.Min, "min_value"),  // 最小值字段的实际字段名为 min_value
        ColumnInfo.customName(Column.Max, "max_value"),  // 最大值字段的实际字段名为 max_value
        ColumnInfo.undefined(Column.Code)                // 表中没有 code 字段
    )
);
```

### 二、使用方法

1. 获得一个 long 类型的序列：`long id = generator.nextLong("seq1")`
1. 获得一个带年月日和编号的字符串序列：`String id = generator.nextSequence("seq1")` 
1. 获得一个带年月日和自定义编号的字符串序列：`String id = generator.nextSequence("seq1", "001")`

使用的示例详见单元测试。

#### 字符串序列的格式

字符串序列的格式为 `yyyyMMdd[code][sequence]`，其中 `[code]` 为 code 字段的值，`[sequence]` 的长度与 max 
字段的值长度相同。例如某个序列，code 字段值为 `888`，max 字段值为 `999999`，那么生成的字符串序列可能为 `"20191231888000001"`。

### 三、兼容 Spring 数据库事务

如果要在 Spring Boot 项目中使用并兼容 Spring 事务，请参考 
`com.hyd.mysqlsequencegenerator.MysqlSequenceGeneratorApplication` 源码示例。

### 四、原理

1. MySQL 支持在 update 语句中使用 `last_insert_id(xxx)` 来临时保存最近生成的 ID 到会话中，接下来可以在同一会话中用
 `select last_insert_id()` 来获取刚生成的 ID。
1. `MysqlSequenceGenerator` 利用 `AtomicLong` 的 `updateAndGet()` 方法来完成计数器更新和数据库操作。
1. `Counter` 是实现并发序列的核心类，它包含一个用于保存自增值的 `AtomicLong` 对象和一个 `Threshold` 
阈值对象，后者表示前者自增到什么值时需要从数据库重新取一个段。取到后，根据取到的最大值将 Threshold 的阈值推高。
1. 当多个线程同时从数据库重新取段时，会分别将 Threshold 的阈值推高数次。

### 四、性能测试

- 10 线程，序列步长  1000，生成 1,000,000 个 ID，耗时 34530 ms
- 10 线程，序列步长 10000，生成 1,000,000 个 ID，耗时  3278 ms

数据库运行在**普通机械硬盘**上，瓶颈都在磁盘 I/O。
