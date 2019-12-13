# mysql-sequence-generator
A mysql-based lock-free id generator using last_insert_id() function.

```sql
create table t_sequence(
  name  varchar(100) primary key             comment '序列名称',
  code  varchar(5) not null default '00000'  comment '序列编号（嵌入最终序列中）',
  value bigint     not null default 0        comment '当前值',
  max   bigint     not null default 99999999 comment '最大值，当 value 达到最大值时重新从 0 开始',
  step  int        not null default 1000     comment '步长，每次取序列时缓存多少，步长越大，数据库访问频率越低'
);

INSERT into t_sequence (name, code, max) values ('seq1', '886', 99999999);
INSERT into t_sequence (name, code, max) values ('seq2', '887', 99999999);
INSERT into t_sequence (name, code, max) values ('seq3', '888', 99999999);
```

基于 MySQL 的 `last_insert_id()` 函数而写的序列生成工具。

效率很高，没有数据库事务。使用方法详见单元测试。

**整个项目只有一个类，且无任何依赖关系，可以直接拷贝到任何项目中使用。**



如果要在 Spring Boot 项目中使用并兼容 Spring 事务，请参考 `com.hyd.mysqlsequencegenerator.MysqlSequenceGeneratorApplication` 源码示例。

性能测试：

- 10 线程，序列步长  1000，生成 1,000,000 个 ID，耗时 34530 ms
- 10 线程，序列步长 10000，生成 1,000,000 个 ID，耗时  3278 ms

数据库运行在普通机械硬盘上，瓶颈都在磁盘 I/O。
