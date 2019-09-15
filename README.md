# mysql-sequence-generator
A mysql-based lock-free id generator using last_insert_id() function.

```sql
create table t_sequence(
  name  varchar(100) primary key,
  value bigint  not null default 0,
  max   bigint  not null default 99999999,
  step  int     not null default 1000
);

INSERT into t_sequence SET name = 'seq1', max = 3000;
```

基于 MySQL 的 `last_insert_id()` 函数而写的序列生成工具。

效率很高，没有数据库事务也没有专门使用锁。有两个地方用到了同步类：

1. 用 `ConcurrentHashMap` 对象来为每个序列名称保存各自的计数器；
2. 在计数器中用 `AtomicLong` 对象来计数。

使用方法详见单元测试。

**整个项目只有一个类，且无任何依赖关系，可以直接拷贝到任何项目中使用。**

如果要在 Spring Boot 项目中使用并兼容 Spring 事务，请参考 `com.hyd.mysqlsequencegenerator.MysqlSequenceGeneratorApplication` 源码示例。
