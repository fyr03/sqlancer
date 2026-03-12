## 一、DDL

### 1. CREATE TABLE

```bash
create_table_stmt
  ::= CREATE TABLE [ IF NOT EXISTS ] table_name LIKE existing_table_name
    | CREATE TABLE [ IF NOT EXISTS ] table_name
        ( column_def { , column_def } )
        [ table_option { , table_option } ]

column_def
  ::= column_name data_type { column_constraint }

data_type
  ::= TINYINT   [ ( display_width ) ] [ UNSIGNED ] [ ZEROFILL ]
    | SMALLINT  [ ( display_width ) ] [ UNSIGNED ] [ ZEROFILL ]
    | MEDIUMINT [ ( display_width ) ] [ UNSIGNED ] [ ZEROFILL ]
    | INT       [ ( display_width ) ] [ UNSIGNED ] [ ZEROFILL ]
    | BIGINT    [ ( display_width ) ] [ UNSIGNED ] [ ZEROFILL ]
    | FLOAT     [ ( M , D ) ]         [ UNSIGNED ] [ ZEROFILL ]
    | DOUBLE    [ ( M , D ) ]         [ UNSIGNED ] [ ZEROFILL ]
    | DECIMAL   [ ( M , D ) ]         [ UNSIGNED ] [ ZEROFILL ]
    | VARCHAR(500)
    | TINYTEXT
    | TEXT
    | MEDIUMTEXT
    | LONGTEXT

column_constraint
  ::= NULL
    | NOT NULL
    | UNIQUE [ KEY ]
    | PRIMARY KEY
    | COMMENT 'string'
    | COLUMN_FORMAT { FIXED | DYNAMIC | DEFAULT }
    | STORAGE { DISK | MEMORY }

table_option
  ::= CHECKSUM { 0 | 1 }
    | ENGINE = { InnoDB | MyISAM | MEMORY | HEAP | CSV | ARCHIVE }
    | INSERT_METHOD = { NO | FIRST | LAST }
    | KEY_BLOCK_SIZE = integer
    | MAX_ROWS = integer
    | MIN_ROWS = integer
    | PACK_KEYS = { 0 | 1 | DEFAULT }
    | STATS_AUTO_RECALC = { 0 | 1 | DEFAULT }
    | STATS_PERSISTENT = { 0 | 1 | DEFAULT }
    | STATS_SAMPLE_PAGES = integer
```

- `ENCRYPTION` 在源码中已被注释掉，不生成。

------

### 2. CREATE INDEX

```bash
create_index_stmt
  ::= CREATE [ UNIQUE ] INDEX index_name
        [ USING { BTREE | HASH } ]
        ON table_name ( index_part { , index_part } )
        [ ALGORITHM [ = ] { DEFAULT | INPLACE | COPY } ]
        [ { VISIBLE | INVISIBLE } ]

index_part
  ::= column_name
    | ( expr )         -- 仅 InnoDB 引擎，为函数式索引
```

------

### 3. ALTER TABLE

```bash
alter_table_stmt
  ::= ALTER TABLE table_name alter_option { , alter_option }
        [ , ORDER BY col_name { , col_name } ]

alter_option
  ::= ALGORITHM [ = ] { INSTANT | INPLACE | COPY | DEFAULT }
    | CHECKSUM { 0 | 1 }
    | COMPRESSION { 'ZLIB' | 'LZ4' | 'NONE' }
    | { DISABLE | ENABLE } KEYS
    | DROP [ COLUMN ] col_name
    | DROP PRIMARY KEY
    | FORCE
    | DELAY_KEY_WRITE { 0 | 1 }
    | INSERT_METHOD { NO | FIRST | LAST }
    | ROW_FORMAT { DEFAULT | DYNAMIC | FIXED | COMPRESSED | REDUNDANT | COMPACT }
    | STATS_AUTO_RECALC { 0 | 1 | DEFAULT }
    | STATS_PERSISTENT { 0 | 1 | DEFAULT }
    | PACK_KEYS { 0 | 1 | DEFAULT }
    | RENAME [ { TO | AS } ] new_table_name
```

- `MySQLAlterTable` 的 Action 枚举中完全没有 ADD COLUMN，只有 DROP COLUMN。

------

## 二、DQL（SELECT）

### 1. SELECT 语句

```bash
select_stmt
  ::= SELECT
        [ /*+ optimizer_hint */ ]
        { DISTINCT | ALL | DISTINCTROW }
        { select_modifier }
        select_item_list
        FROM table_ref { , table_ref }
             { join_clause }
        [ WHERE expr ]
        [ GROUP BY expr { , expr } ]
        [ ORDER BY order_term { , order_term } ]
        [ LIMIT expr [ OFFSET expr ] ]

select_modifier
  ::= STRAIGHT_JOIN
    | SQL_SMALL_RESULT
    | SQL_BIG_RESULT
    | SQL_NO_CACHE

select_item_list
  ::= *
    | expr AS alias { , expr AS alias }

join_clause
  ::= join_type JOIN table_name [ ON expr ]

join_type
  ::= NATURAL
    | INNER
    | STRAIGHT_         -- 序列化时为 "STRAIGHT_JOIN"
    | LEFT
    | RIGHT
    | CROSS

order_term
  ::= expr { ASC | DESC }
```

- `SelectBase` 中存在 `havingClause` 字段，CERT Oracle 也定义了 `mutateHaving()`，但 `MySQLToStringVisitor` 的 `visit(MySQLSelect)` 方法中没有 HAVING 的序列化逻辑，因此 HAVING 实际上不会出现在 SQLancer MySQL 生成的 SQL 中。

- 列别名，每个 fetch 列都会追加 `AS ref{N}`（N 自增），这是 SQLancer 防止 MySQL 重复列名报错的内部实现，并非标准 BNF 的一部分。

------

### 2. 表达式

```bash
expr
  ::= column_ref
    | literal
    | unary_prefix_op  expr
    | expr  unary_postfix_op
    | scalar_function
    | aggregate_function
    | expr  binary_logical_op  expr
    | expr  comparison_op  expr
    | expr  bit_op  expr
    | CAST ( expr AS cast_type )
    | expr [ NOT ] IN ( expr { , expr } )
    | EXISTS ( select_stmt )
    | ( expr ) BETWEEN ( expr ) AND ( expr )
    | CASE [ expr ] WHEN expr THEN expr
            { WHEN expr THEN expr }
            [ ELSE expr ]
      END
    | ( expr COLLATE 'collation_name' )
```

------

### 3. 操作符

```bash
unary_prefix_op
  ::= +
    | -
    | !       -- 逻辑非（与 NOT 等价，随机选其一）
    | NOT

unary_postfix_op
  ::= IS [ NOT ] TRUE
    | IS [ NOT ] FALSE
    | IS [ NOT ] { NULL | UNKNOWN }

binary_logical_op
  ::= AND | &&        -- 随机选其一
    | OR  | ||        -- 随机选其一
    | XOR

comparison_op
  ::= =  | !=  | <  | <=  | >  | >=  | LIKE

bit_op
  ::= &     -- 按位 AND
    | |     -- 按位 OR
    | ^     -- 按位 XOR

cast_type
  ::= SIGNED
```

- 源码中 `CastType` 枚举包含 `SIGNED` 和 `UNSIGNED`，但 `CastType.getRandom()` 方法写死了 `return SIGNED`，因此实际只生成 `CAST(... AS SIGNED)`。

- `<=>` 空安全等于未实现，源码中明确注释 `/* IS_EQUALS_NULL_SAFE("<=>"){ ... } */`，因 MySQL bug #95908 被排除。

- `BinaryComparisonOperator` 中只有 `LIKE`，没有 `NOT LIKE`。

------

### 4. 标量函数

```bash
scalar_function
  ::= BIT_COUNT    ( expr )
    | COALESCE     ( expr { , expr } )
    | IF           ( expr , expr , expr )
    | IFNULL       ( expr , expr )
    | LEAST        ( expr { , expr } )
    | GREATEST     ( expr { , expr } )
```

- `ABS`、`BENCHMARK` 在源码中已被注释掉，未实现。

------

### 5. 聚合函数

```bash
aggregate_function
  ::= COUNT  ( expr )
    | COUNT  ( DISTINCT expr { , expr } )
    | SUM    ( expr )
    | SUM    ( DISTINCT expr )
    | MIN    ( expr )
    | MIN    ( DISTINCT expr )
    | MAX    ( expr )
    | MAX    ( DISTINCT expr )
```

- `AVG` 未实现。聚合函数不能在 PQS Oracle 中使用（`MySQLExpectedValueVisitor` 对 aggregate 直接抛出 `IgnoreMeException`）。

------

### 6. 字面值

```bash
literal
  ::= integer      -- 有符号 / 无符号 long
    | float
    | string
    | NULL
    | TRUE
    | FALSE
```

------

### 7. Optimizer Hints（仅 DQP Oracle 使用）

```bash
optimizer_hint
  ::= hint_name [ ( table_name { , table_name } ) ]

hint_name
  ::= BKA | NO_BKA | BNL | NO_BNL
    | DERIVED_CONDITION_PUSHDOWN | NO_DERIVED_CONDITION_PUSHDOWN
    | GROUP_INDEX | NO_GROUP_INDEX
    | HASH_JOIN | NO_HASH_JOIN
    | INDEX | NO_INDEX
    | INDEX_MERGE | NO_INDEX_MERGE
    | JOIN_FIXED_ORDER
    | JOIN_INDEX | NO_JOIN_INDEX
    | JOIN_ORDER | JOIN_PREFIX | JOIN_SUFFIX
    | MERGE | NO_MERGE
    | MRR | NO_MRR | NO_ICP
    | NO_RANGE_OPTIMIZATION
    | ORDER_INDEX | NO_ORDER_INDEX
    | SEMIJOIN | NO_SEMIJOIN
    | SKIP_SCAN | NO_SKIP_SCAN
```