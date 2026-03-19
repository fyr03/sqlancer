# MySQLSubsetOracle 设计与代码流程文档

------

## 1. 整体设计思路

### 核心思想：子集单调性（Subset Monotonicity）

SubsetOracle 基于**数据库子集关系下的查询单调性**这一数学性质：若 S1 ⊆ S2（S1 的每一行都出现在 S2 中），则对相同的查询，以下性质必然成立：

| 聚合/查询              | 单调性方向                              | 理由                                    |
| ---------------------- | --------------------------------------- | --------------------------------------- |
| `COUNT(*)`             | COUNT(S1) ≤ COUNT(S2)                   | S2 的行数不少于 S1                      |
| `MAX(col)`             | MAX(S1) ≤ MAX(S2)                       | S2 包含 S1 所有行，极大值只会更大或相等 |
| `MIN(col)`             | MIN(S1) ≥ MIN(S2)                       | S2 包含 S1 所有行，极小值只会更小或相等 |
| `EXISTS`               | EXISTS(S1) ⟹ EXISTS(S2)                 | S1 非空则 S2 也非空                     |
| `COUNT(DISTINCT col)`  | COUNT_DISTINCT(S1) ≤ COUNT_DISTINCT(S2) | S2 涵盖 S1 的所有不同值                 |
| `SELECT ... WHERE ...` | result(S1) ⊆ result(S2)                 | S1 满足条件的行在 S2 中同样满足         |
| `IN 子查询`            | IN_count(S1) ≤ IN_count(S2)             | S2 的匹配集合更大或相等                 |

若检测到违反上述性质的情况，则判定为 MySQL 存在 **bug**。

### 两种 Oracle 的子集构造方法

```
Oracle 1 (MySQLSubsetOracle):
  Step 1: 随机生成 S1（随机 DDL + 随机 DML）
  Step 2: 创建 S2 = copy(S1)，再向 S2 额外 INSERT 数据
  结论: S1 ⊆ S2

Oracle 2 (MySQLSubsetOracle2):
  Step 1: 随机生成 S1（随机 DDL + 随机 DML）
  Step 2: 从 S1 随机采样约 50% 的行插入空表 S2
  结论: S2 ⊆ S1
```

------

## 2. Oracle 1：MySQLSubsetOracle（Copy + Extra Insert 方法）

### 整体流程

```
check()
├── Step 1: 随机创建表 S1（MySQLTableGenerator）
├── Step 2: 向 S1 随机插入数据（INSERT + DELETE + UPDATE + 边界值噪音）
│           └── 所有 DML SQL 记录到 s1SqlLog
├── Step 3: 创建 S2（LIKE S1），重放 s1SqlLog 到 S2
│           └── 验证 COUNT(S1) == COUNT(S2)，确保完整复制
├── Step 4: 仅向 S2 插入额外随机行（INSERT IGNORE）
├── Step 5: 验证各类单调性属性
│   ├── verifyCount()         —— COUNT(*) 单调性
│   ├── verifyExists()        —— EXISTS 单调性
│   ├── verifyMax()           —— MAX 单调性（每个数值列）
│   ├── verifyMin()           —— MIN 反单调性（每个数值列）
│   ├── verifyCountDistinct() —— COUNT(DISTINCT) 单调性（每个数值列）
│   ├── verifySelectSubset()  —— SELECT 结果子集关系
│   └── verifyInSubquery()    —— IN 子查询单调性
└── finally: 清理临时表 S1, S2
```

### Step 1：表创建

```java
// 避免生成 "CREATE TABLE sX LIKE tY" 形式的 DDL
// 因为 LIKE 语法会直接复制已有表，无法随机控制 Schema
SQLQueryAdapter createS1;
do {
    createS1 = MySQLTableGenerator.generate(state, s1Name);
} while (createS1.getQueryString().toUpperCase().contains(" LIKE "));
```

**关键约束**：过滤掉包含 `LIKE` 的 DDL，保证每次都是从零生成一个随机 schema 的表。

### Step 2：S1 数据填充

填充操作分三类，按概率随机选择：

```
60%: INSERT — 使用 MySQLInsertGenerator（可能生成 REPLACE...INTO）
20%: DELETE — 直接构造 DELETE FROM s1 WHERE <随机谓词>
20%: UPDATE — 直接构造 UPDATE s1 SET <列>=<随机值> WHERE <随机谓词>
```

操作次数：`100 + Randomly.smallNumber()`（smallNumber 由高斯分布决定，通常为个位数）。

此外还调用 `insertNoiseRows(s1Table, s1SqlLog)` 插入边界值和 NULL 噪音行，覆盖极端情况。

所有执行过的 SQL 语句都记录在 `s1SqlLog` 列表中（包括噪音行的 INSERT），用于 Step 3 重放。

### Step 3：复制 S1 到 S2

```sql
-- 创建同构空表
CREATE TABLE s2_sub_{id} LIKE s1_sub_{id};

-- 重放 S1 的所有 DML（将 SQL 中的 s1Name 替换为 s2Name）
-- 例如: INSERT INTO s1_sub_1 (...) VALUES (...)
--  变为: INSERT INTO s2_sub_1 (...) VALUES (...)
```

重放后验证行数严格相等（`COUNT(S1) == COUNT(S2)`）。若不相等（例如 UNIQUE 约束导致部分行在 S2 中被拒绝），则本轮跳过，不作为 bug 上报。

### Step 4：S2 额外插入

```java
// INSERT IGNORE 保证不因约束冲突报错
// 仅对 S2 插入，S1 不变，维持 S1 ⊆ S2 的不变量
int nrS2Extra = 25 + Randomly.smallNumber();
// 生成方式: INSERT IGNORE INTO s2Name (cols) VALUES (random_consts)
```

------

## 3. Oracle 2：MySQLSubsetOracle2（随机行采样方法）

### 整体流程

```
check()
├── Step 1: 随机创建表 S1（MySQLTableGenerator）
├── Step 2: 向 S1 随机插入数据（INSERT + DELETE + UPDATE + 边界值噪音）
├── Step 3: 创建 S2（LIKE S1），通过 SQL 级随机采样获取 S1 约 50% 的行
│           → INSERT IGNORE INTO S2 SELECT * FROM S1 WHERE RAND() < 0.5
├── Step 4: 验证各类单调性属性（注意：此时 S2 ⊆ S1，参数顺序与 Oracle 1 相反）
│   ├── verifyCount(s2, s1, ...)
│   ├── verifyExists(s2, s1, ...)
│   ├── verifyMax(s2, s1, ...)
│   ├── verifyMin(s2, s1, ...)
│   ├── verifyCountDistinct(s2, s1, ...)
│   ├── verifySelectSubset(s2, s1, ...)
│   └── verifyInSubquery(s2, s1, ...)
└── finally: 清理临时表 S1, S2
```

### Step 3 采样 SQL

```sql
INSERT IGNORE INTO s2_sub2_{id}
SELECT * FROM s1_sub2_{id} WHERE RAND() < 0.5;
```

- `RAND() < 0.5` 实现约 50% 的行级随机采样（实际比例随机波动）
- `INSERT IGNORE` 防止因 UNIQUE 约束导致的插入失败
- **不验证精确行数**，只要 S2 是 S1 的子集，单调性验证就有效

### 参数顺序的差异

Oracle 2 中 S2 ⊆ S1，所以验证函数调用的 "小集合" 和 "大集合" 参数与 Oracle 1 相反：

```java
// Oracle 1: S1 ⊆ S2，以 s1 为小集合
verifyCount(s1Name, s2Name, ...)

// Oracle 2: S2 ⊆ S1，以 s2 为小集合
verifyCount(s2Name, s1Name, ...)
```

------

## 4. 表 Schema 的随机生成（Approximate Schema）

Schema 的随机生成由 `MySQLTableGenerator` 完成，生成的 schema 覆盖 MySQL 的多种数据类型和列约束组合。

### 4.1 数据类型

```
MySQL 数据类型（MySQLDataType）:
  INT     → TINYINT / SMALLINT / MEDIUMINT / INT / BIGINT
            可选: (显示宽度), UNSIGNED, ZEROFILL
  VARCHAR → VARCHAR(500) / TINYTEXT / TEXT / MEDIUMTEXT / LONGTEXT
  FLOAT   → FLOAT[(M,D)], 可选 UNSIGNED ZEROFILL
  DOUBLE  → DOUBLE[(M,D)] 或 FLOAT[(M,D)]
  DECIMAL → DECIMAL[(M,D)]
```

### 4.2 列约束（Column Options）

```
ColumnOptions（随机子集）:
  NULL_OR_NOT_NULL  → NULL | NOT NULL
  UNIQUE            → UNIQUE [KEY]
  COMMENT           → COMMENT 'asdf'
  COLUMN_FORMAT     → COLUMN_FORMAT {FIXED|DYNAMIC|DEFAULT}
  STORAGE           → STORAGE {DISK|MEMORY}
  PRIMARY_KEY       → PRIMARY KEY（仅当 allowPrimaryKey && !setPrimaryKey && !isNull）
```

### 4.3 表级选项（Table Options）

```
TableOptions（随机子集，通常只选少数几项）:
  AUTO_INCREMENT, AVG_ROW_LENGTH, CHECKSUM, COMPRESSION,
  DELAY_KEY_WRITE, ENGINE, INSERT_METHOD, KEY_BLOCK_SIZE,
  MAX_ROWS, MIN_ROWS, PACK_KEYS, STATS_AUTO_RECALC,
  STATS_PERSISTENT, STATS_SAMPLE_PAGES
```

### 4.4 存储引擎（ENGINE）

```
ENGINE ∈ {InnoDB, MyISAM, MEMORY, HEAP, CSV, ARCHIVE}
```

### 4.5 分区选项（Partition，仅 InnoDB）

```
PARTITION BY {
    [LINEAR] HASH(column)
  | [LINEAR] KEY [ALGORITHM={1|2}] (column_list)
}
```

### 4.6 完整 DDL 的近似 BNF

```bnf
create_table ::= CREATE [TABLE] [IF NOT EXISTS] table_name
                 '(' column_def (',' column_def)* ')'
                 [table_options]
                 [partition_options]

column_def ::= column_name data_type [column_constraint]*

data_type ::= INT_TYPE [display_width] [UNSIGNED] [ZEROFILL]
            | VARCHAR_TYPE
            | FLOAT_TYPE [(M,D)] [UNSIGNED] [ZEROFILL]
            | DOUBLE_TYPE [(M,D)] [UNSIGNED] [ZEROFILL]
            | DECIMAL [(M,D)] [UNSIGNED] [ZEROFILL]

INT_TYPE     ::= TINYINT | SMALLINT | MEDIUMINT | INT | BIGINT
VARCHAR_TYPE ::= VARCHAR(500) | TINYTEXT | TEXT | MEDIUMTEXT | LONGTEXT
FLOAT_TYPE   ::= FLOAT
DOUBLE_TYPE  ::= DOUBLE | FLOAT

column_constraint ::= (NULL | NOT NULL)
                    | UNIQUE [KEY]
                    | COMMENT 'string'
                    | COLUMN_FORMAT (FIXED|DYNAMIC|DEFAULT)
                    | STORAGE (DISK|MEMORY)
                    | PRIMARY KEY

table_options ::= option (',' option)*
option ::= ENGINE = engine_name
         | AUTO_INCREMENT = int
         | COMPRESSION = ('ZLIB'|'LZ4'|'NONE')
         | AVG_ROW_LENGTH = int
         | ...（其余见 TableOptions 枚举）

partition_options ::= PARTITION BY
                      ([LINEAR] HASH '(' column ')' |
                       [LINEAR] KEY [ALGORITHM=N] '(' column_list ')')
```

------

## 5. 查询的 BNF 文法

### 5.1 随机表达式（WHERE 谓词）

由 `MySQLExpressionGenerator.generateExpression()` 递归生成，深度受 `maxExpressionDepth` 限制：

```bnf
expr ::= column_ref
       | constant
       | unary_prefix_op expr
       | expr unary_postfix_op
       | expr binary_logical_op expr
       | expr binary_comparison_op expr
       | cast_expr
       | in_expr
       | bitwise_binary_expr
       | exists_expr
       | between_expr
       | case_expr
       | computable_function_call

unary_prefix_op      ::= NOT | ! | + | -
unary_postfix_op     ::= IS [NOT] NULL | IS [NOT] TRUE | IS [NOT] FALSE

binary_logical_op    ::= AND | && | OR | || | XOR

binary_comparison_op ::= = | != | < | <= | > | >= | LIKE

bitwise_binary_expr  ::= expr ('&' | '|' | '^') expr

cast_expr   ::= CAST(expr AS (SIGNED | UNSIGNED))
in_expr     ::= expr [NOT] IN '(' expr (',' expr)* ')'
between_expr::= expr BETWEEN expr AND expr
exists_expr ::= EXISTS '(' subquery ')'
case_expr   ::= CASE [expr] (WHEN expr THEN expr)+ [ELSE expr] END

computable_function_call ::= func_name '(' expr (',' expr)* ')'
func_name ::= BIT_COUNT | COALESCE | IF | IFNULL | LEAST | GREATEST

constant ::= int_literal | NULL | string_literal | double_literal
```

### 5.2 SELECT 查询（verifySelectSubset 中生成）

```bnf
select_stmt ::= SELECT [DISTINCT | ALL | DISTINCTROW]
                fetch_columns
                FROM table_ref [join_clause]
                [WHERE expr]

fetch_columns ::= column_ref (',' column_ref)*

join_clause ::= join_type JOIN table_name ON expr
join_type   ::= INNER | LEFT

table_ref ::= table_name
```

**注意**：`verifySelectSubset` 中 GROUP BY 和 HAVING 子句被注释掉，因为它们可能破坏子集单调性——聚合后的结果集是分组摘要，不再是原始行的子集关系。

### 5.3 聚合查询（verifyCount / verifyMax / verifyMin）

```bnf
count_query  ::= SELECT COUNT(*) FROM table_name [WHERE expr]
max_query    ::= SELECT MAX(`col`) FROM table_name [WHERE expr]
min_query    ::= SELECT MIN(`col`) FROM table_name [WHERE expr]
exists_query ::= SELECT EXISTS(SELECT 1 FROM table_name LIMIT 1)
count_dist_q ::= SELECT COUNT(DISTINCT `col`) FROM table_name

in_subquery  ::= SELECT COUNT(*) FROM outer_table
                 WHERE `outer_col` IN (SELECT `inner_col` FROM inner_table)
```

### 5.4 数据插入（DML）

```bnf
insert_stmt ::= (INSERT | REPLACE)
                [LOW_PRIORITY | DELAYED | HIGH_PRIORITY]
                [IGNORE]
                INTO table_name '(' col_list ')'
                VALUES row_values (',' row_values)*

row_values ::= '(' value (',' value)* ')'
value      ::= int_literal | NULL | string_literal | double_literal

delete_stmt ::= DELETE FROM table_name WHERE expr

update_stmt ::= UPDATE table_name SET `col` = constant WHERE expr
```

------

## 6. 各验证方法的实现细节

### 6.1 verifyCount

```java
// 随机决定是否添加 WHERE 子句（两张表共用相同的 WHERE 字符串）
String where = Randomly.getBoolean()
    ? " WHERE " + MySQLVisitor.asString(gen.generateExpression())
    : "";

String q1 = "SELECT COUNT(*) FROM " + s1Name + where;
String q2 = q1.replaceAll("\\b" + s1Name + "\\b", s2Name);
```

**关键点**：WHERE 子句通过字符串替换从 q1 复用到 q2，保证两张表使用完全相同的过滤条件，在子集关系下 COUNT 单调性依然成立。

验证条件：`COUNT(S1_filtered) ≤ COUNT(S2_filtered)`

### 6.2 verifyMax / verifyMin

与 `verifyCount` 类似，也支持随机 WHERE 子句。

**MAX 验证**：

- 若 S1 结果为 NULL（表为空或过滤后无行），则直接 PASS（vacuously true）
- 否则验证 `MAX(S1) ≤ MAX(S2) + 1e-9`（浮点数容差）

**MIN 验证**：

- 若 S1 结果为 NULL，则直接 PASS
- 否则验证 `MIN(S2) ≤ MIN(S1) + 1e-9`

### 6.3 verifyExists

直接利用已知行数缓存，不发出额外 SQL：

```java
boolean e1 = (knownC1 != null) ? knownC1 > 0
    : executeSingleLong("SELECT EXISTS(SELECT 1 FROM " + s1Name + " LIMIT 1)") == 1L;
boolean e2 = (knownC2 != null) ? knownC2 > 0
    : executeSingleLong("SELECT EXISTS(SELECT 1 FROM " + s2Name + " LIMIT 1)") == 1L;
```

验证条件：`!e1 || e2`（即 e1 ⟹ e2 的逻辑蕴含）

### 6.4 verifySelectSubset —— 行集合比较（非 EXCEPT）

这是最复杂的验证方法，也是一个重要的实现细节。

**验证方式**：在 Java 层将查询结果读出并做集合包含判断，而**不是**用 SQL 的 `EXCEPT` / `NOT IN` 语句。

```java
Set<String> rows1 = executeAndGetRowSet(q1, s1Table.getColumns().size());
Set<String> rows2 = executeAndGetRowSet(q2, s1Table.getColumns().size());

Set<String> missing = new LinkedHashSet<>(rows1);
missing.removeAll(rows2);  // rows1 \ rows2

if (!missing.isEmpty()) {
    throw new AssertionError("SELECT subset violation: result(S1) ⊄ result(S2)...");
}
```

行的序列化方式：将每行的所有列用 `|` 拼接成字符串作为 key：

```java
StringBuilder row = new StringBuilder();
for (int i = 1; i <= colCount; i++) {
    if (i > 1) row.append("|");
    row.append(rs.getString(i));   // NULL 值序列化为 Java 的 "null" 字符串
}
rows.add(row.toString());
```

**使用 Java 集合而非 SQL EXCEPT 的原因**：

1. `EXCEPT` 是集合操作，自动去重（类似 UNION 而非 UNION ALL），可能掩盖某些重复行级别的差异
2. 避免生成一条同时引用 S1 和 S2 的复合查询，保持查询的独立性
3. 便于在报错时直接展示具体缺失的行内容

**事务保证**：两次查询包裹在 `REPEATABLE READ` 事务中，避免并发修改导致误报：

```java
state.executeStatement(new SQLQueryAdapter(
    "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", true));
state.executeStatement(new SQLQueryAdapter("ROLLBACK", true));
state.executeStatement(new SQLQueryAdapter("START TRANSACTION", true));
try {
    rows1 = executeAndGetRowSet(q1, ...);
    rows2 = executeAndGetRowSet(q2, ...);
} catch (SQLException e) {
    // 查询执行失败（如算术溢出 ERROR 1690），跳过本次检查，不当 bug 上报
    queryFailed = true;
} finally {
    state.executeStatement(new SQLQueryAdapter("ROLLBACK", true));
}
if (queryFailed) continue;
```

**查询执行失败的处理**：若 q1 或 q2 执行失败（如 `ERROR 1690: BIGINT value is out of range`），不当作 bug 处理，而是 `continue` 到下一次迭代。

**JOIN 支持**：`verifySelectSubset` 支持随机引入全局表（非 S1/S2 的现有表）作为 JOIN 对象：

```java
MySQLTable joinTable = (!globalTables.isEmpty() && Randomly.getBoolean())
    ? Randomly.fromList(globalTables) : null;
// JOIN 类型: INNER JOIN 或 LEFT JOIN（随机选择）
```

**不启用的特性（注释掉）**：GROUP BY 和 HAVING 子句暂时被注释掉，因为它们会破坏行级子集关系——聚合后结果是分组摘要，不再满足"S1 的每一行都出现在 S2 中"的子集语义。

### 6.5 verifyCountDistinct

```java
String q1 = "SELECT COUNT(DISTINCT `" + col + "`) FROM " + s1Name;
String q2 = "SELECT COUNT(DISTINCT `" + col + "`) FROM " + s2Name;
```

验证 `COUNT(DISTINCT, S1) ≤ COUNT(DISTINCT, S2)`，此版本不添加 WHERE 子句。

### 6.6 verifyInSubquery

用全局现有表作为外层驱动表，测试 IN 子查询在子集关系下的单调性：

```java
// S1 是子集 → IN(S1) 匹配的行数 ≤ IN(S2) 匹配的行数
String q1 = "SELECT COUNT(*) FROM outer_table WHERE `col` IN (SELECT `col` FROM s1)";
String q2 = "SELECT COUNT(*) FROM outer_table WHERE `col` IN (SELECT `col` FROM s2)";
```

同样包裹在事务中，保证 outer_table 对两次查询的快照一致。

------

## 7. 关键实现细节与设计决策

### 7.1 边界值噪音注入（insertNoiseRows）

每列单独测试一组边界值，其他列填 NULL，每次一行：

| 数据类型     | 边界值                                                       |
| ------------ | ------------------------------------------------------------ |
| INT          | 0, 1, -1, TINYINT_MIN/MAX, SMALLINT_MIN/MAX, INT_MIN/MAX, BIGINT_MIN/MAX, NULL |
| VARCHAR      | 空字符串, 500字符串, `%`, `_`, `NULL`（字符串），`0`, NULL   |
| FLOAT/DOUBLE | 0, 0.0, -0.0, 1.0, -1.0, FLOAT_MAX, DOUBLE_MAX, 最小正值, NULL |
| DECIMAL      | 0, 0.00, 1.00, -1.00, 超大值, NULL                           |

最后额外插入一行全 NULL 行，测试 NULL 处理逻辑。

**Oracle 1 的特殊处理**：噪音行的 SQL 也记入 `s1SqlLog`，会被重放到 S2，确保两表完全一致。

### 7.2 表名唯一性

使用原子计数器生成每轮唯一的表名，防止多线程冲突：

```java
private static final AtomicInteger TABLE_COUNTER = new AtomicInteger(0);
int id = TABLE_COUNTER.incrementAndGet();
String s1Name = "s1_sub_" + id;    // Oracle 1
String s2Name = "s2_sub_" + id;
// Oracle 2 使用 "s1_sub2_" / "s2_sub2_" 前缀，与 Oracle 1 区分
```

### 7.3 try-finally 清理机制

`finally` 块保证即使验证失败也会清理临时表，防止表数量无限积累：

```java
finally {
    dropIfExists(s2Name);
    dropIfExists(s1Name);
    state.updateSchema();
}
```

### 7.4 异常处理分层

```
AssertionError                      → 上报 bug，中断本轮，向外传播
IgnoreMeException                   → 跳过本轮（正常情况，非 bug）
SQLNonTransientConnectionException
CommunicationsException
SQLRecoverableException             → 连接断开，包装为 IgnoreMeException
SQLException (含 NumberFormatException) → MySQL bug114533 workaround，IgnoreMeException
其他 Throwable（verifySelectSubset 内部）→ 记录日志，跳过本次 SELECT 检查，继续循环
```

### 7.5 WHERE 子句的共用策略

verifyCount、verifyMax、verifyMin 中 WHERE 谓词生成一次后，通过字符串替换同时应用到 S1 和 S2：

```java
String q2 = q1.replaceAll("\\b" + s1Name + "\\b", s2Name);
```

这利用了正则边界符 `\b`，精确匹配完整的表名，避免部分替换。在相同结构的两张表上执行同一谓词，子集单调性依然成立。

### 7.6 DML 操作的目标控制

Step 2 中 DELETE 和 UPDATE 操作**直接构造 SQL 字符串**，而不是通过 `MySQLDeleteGenerator` 或 `MySQLUpdateGenerator`：

```java
// DELETE 直接指定目标表名，不受 globalState.schema.getRandomTable() 随机性影响
String deleteSql = "DELETE FROM " + s1Name + " WHERE " + ...;
// UPDATE 同理
String updateSql = "UPDATE " + s1Name + " SET `" + col + "` = ... WHERE " + ...;
```

这样确保 DML 精准操作 S1，而不会误操作全局 schema 中的其他表。

------

## 8. 两种 Oracle 的对比

| 特性                 | Oracle 1 (SubsetOracle)           | Oracle 2 (SubsetOracle2)           |
| -------------------- | --------------------------------- | ---------------------------------- |
| 子集构造方式         | 完整复制 S1 数据到 S2，再额外插入 | SQL 层随机采样 S1 的约 50% 行到 S2 |
| 子集方向             | S1 ⊆ S2                           | S2 ⊆ S1                            |
| S1 数据填充次数      | 100 + small                       | 200 + small                        |
| S2 额外数据          | 额外 25+ 行随机插入               | 无（S2 仅是 S1 的子样本）          |
| 对 UNIQUE 约束的处理 | 复制行数不一致则跳过本轮          | INSERT IGNORE 忽略冲突             |
| 表名前缀             | s1_sub_ / s2_sub_                 | s1_sub2_ / s2_sub2_                |