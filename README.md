# mysql-sequence-generator
A mysql-based lock-free id generator using last_insert_id() function.

```sql
create table t_sequence(
  name  varchar(100) primary key,
  value bigint  not null default 0,
  max   bigint  not null default 99999999,
  step  int     not null default 1000
);

-- test purpose
INSERT into t_sequence SET name = 'seq1', max = 3000;
```