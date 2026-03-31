# MySQLSubsetOracle3 设计与代码流程


## 1. 概述

`MySQLSubsetOracle3` 是一个针对 MySQL 的时态子集 Oracle。

与 Oracle 1 不同，它不维护两张独立的表 `S1` 和 `S2`，而是利用同一张表在两个不同时态下的状态：

- `S1`：插入少量数据后的表状态
- `S2`：在 `S1` 基础上追加大量行并执行 `ANALYZE TABLE` 刷新优化器统计信息后的表状态

核心思想如下：
```
S1 是 S2 的子集
因为 S2 仅由向 S1 追加行生成
不涉及 DELETE 或 UPDATE
```

因此，对于同一条查询 `Q`，以下单调性约束应成立：

| 查询 / 聚合 | 预期关系 |
| --- | --- |
| `COUNT(*)` | `COUNT(S1) <= COUNT(S2)` |
| `MAX(col)` | `MAX(S1) <= MAX(S2)` |
| `MIN(col)` | `MIN(S1) >= MIN(S2)` |
| `SELECT ... WHERE ...` | `Res1 ⊆ Res2` |

若上述任一约束被违反，Oracle3 即报告一个逻辑缺陷。

Oracle3 还会通过 `EXPLAIN` 记录 `Plan1` 和 `Plan2`，因为以下情形的缺陷尤为值得关注：

```text
结果发生错误的同时，优化器恰好切换了执行计划
```

------

## 2. Oracle3 的设计动机

Oracle 1 通过两张不同的物理表构建子集关系。Oracle3 延续了子集测试的核心理念，但将子集关系转移到时间维度上：

```text
同一张表，同一条查询，不同的表状态
```

这种方式的价值在于：许多优化器缺陷在以下场景中被触发：

1. 表初始较小
2. 优化器选择某一执行计划
3. 插入大量行
4. 刷新统计信息
5. 优化器切换到另一执行计划

因此，Oracle3 主要测试：

- 基数估计的变化
- 索引扫描与全表扫描的切换
- 倾斜插入后选择率的变化
- 计划切换过程中的逻辑正确性

------

## 3. high-level 执行流程

```text
check()
  1：创建一张临时表
  2：构建小规模基线状态 S1
  3：生成一个或多个基线查询，记录 Res1 / Plan1
  4：追加大量倾斜行，执行 COMMIT 和 ANALYZE TABLE，形成 S2
  5：在 S2 上重新执行相同查询，记录 Res2 / Plan2
  6：验证单调性与行集合包含关系
  finally：删除临时表
```

------

## 4. 逐步执行说明

### 步骤 1：创建临时表

Oracle3 首先创建一张全新的随机表：

```java
createTable = MySQLTableGenerator.generate(state, tableName);
```

它会显式跳过 `CREATE TABLE ... LIKE ...` 形式，因为 Oracle3 每轮都需要一个全新的随机模式（schema）。

建表后执行以下操作：

- 刷新 schema 缓存
- 从 schema 中查找该表
- 收集数值列，用于后续的 `MAX` 和 `MIN` 检查
- 选定一列作为谓词列

谓词列至关重要，因为它驱动以下操作：

- 查询生成
- 倾斜数据生成
- 索引创建

### 步骤 2：构建基线状态 `S1`

Oracle3 接着构建小规模的表状态 `S1`。

这里刻意不进行大规模随机填充。目的是保持 `S1` 足够小，使优化器在过渡到 `S2` 之前可能选择不同的执行策略。

基线状态由三部分构成：

1. hot seed行
2. 少量倾斜随机行
3. 少量噪声 / 边界行

#### 2.1 热点种子行

Oracle3 插入若干行，将谓词列强制设为选定的热点值：

```text
predicate_col = primaryHotValue
```

这确保后续基线查询大概率能匹配到数据。

#### 2.2 小规模倾斜随机填充

Oracle3 接着插入少量带有偏置值的行。

在 `BASELINE` 阶段，谓词列的值高度集中于：

- 主热点值
- 次热点值
- 三级热点值

这使得 `S1` 规模虽小，但并非完全均匀分布。

#### 2.3 噪声行

Oracle3 还会插入若干边界行，例如：

- `0`、`1`、`-1`
- 大整数边界值
- `0.0`、`-0.0`、极端浮点数
- 空字符串
- 类通配符字符串，如 `'%'` 和 `'_'`
- `NULL`

这些行有助于暴露以下场景的正确性缺陷：

- NULL 处理
- 边界算术运算
- 比较运算的边角情况

### 步骤 3：生成基线查询并记录 `Res1 / Plan1`

Oracle3 不依赖单条基线查询，而是在一轮中尝试构建多条经过验证的基线查询。

核心方法为：

```java
createValidatedBaselinePhases(...)
```

它反复生成候选查询，直到收集到足够数量的可用基线。

每条基线存储以下内容：

- 查询文本
- `COUNT` 值
- 结果集的行摘要（row digests）
- 数值型 `MAX` 值
- 数值型 `MIN` 值
- `EXPLAIN` 执行计划

若候选查询在基线阶段返回空结果，则跳过该查询。这一点至关重要，因为 Oracle3 需要有意义的子集检查。

### 步骤 4：将 `S1` 扩展为 `S2`

记录好 `S1` 的基线查询后，Oracle3 追加大量行：

```java
START TRANSACTION
appendSkewedRows(..., DistributionStage.EXPANSION)
COMMIT
ANALYZE TABLE
```

这是 Oracle3 的核心步骤。

重要细节：

- 仅使用 `INSERT`
- 不使用 `DELETE`
- 不使用 `UPDATE`
- 因此子集关系始终成立

关系为：

```text
S1 ⊆ S2
```

插入完成后，Oracle3 会验证表是否充分增长。若增长量过小，则跳过本轮，因为该状态转换没有测试价值。

最后，`ANALYZE TABLE` 刷新优化器统计信息，使 MySQL 有机会重新选择执行计划。

### 步骤 5：在 `S2` 上重新执行查询

Oracle3 在更大的表状态 `S2` 上再次执行相同的基线查询。

对每条查询记录：

- `COUNT` 值
- 数值型 `MAX`
- 数值型 `MIN`
- `EXPLAIN` 执行计划

然后比较 `Plan1` 与 `Plan2`。

计划变化不是报告缺陷的必要条件，但它是强有力的诊断证据。

### 步骤 6：验证单调性

对每条基线查询，Oracle3 逐项检查：

1. `COUNT(S1) <= COUNT(S2)`
2. 对每个数值列，`MAX(S1) <= MAX(S2)`
3. 对每个数值列，`MIN(S1) >= MIN(S2)`
4. `Res1 ⊆ Res2`

若任一检查失败，Oracle3 抛出 `AssertionError` 并报告缺陷。

------

## 5. 查询生成

Oracle3 使用 `QuerySpec` 对象表示一条测试查询。

其中存储：

- 目标表名
- `WHERE` 子句
- `SELECT` 查询文本
- 投影列
- 选定的谓词列

### 5.1 谓词形式

`WHERE` 子句由选定的谓词列和倾斜配置生成。

对于数值列，Oracle3 可能生成：

```sql
WHERE col = hot_value
WHERE col IN (hot1, hot2)
WHERE col BETWEEN hot1 AND hot2
WHERE col <= hot2
WHERE col >= hot1
WHERE col IS NULL
```

对于字符串列，Oracle3 可能生成：

```sql
WHERE col = 'hot'
WHERE col IN ('hot', 'skew')
WHERE col LIKE 'h%'
WHERE col >= 'hot'
WHERE col IS NULL
```

这比单一的等值谓词更丰富，能显著提升触发计划变化的概率。

### 5.2 投影方式

Oracle3 同样会对投影列进行变化。

可能的投影方式包括：

- 所有列
- 仅谓词列
- 谓词列加一个额外列
- 随机非空子集
- 以非谓词列为主的子集

这有助于在不同结果形态下测试行集合包含关系，而不仅限于全列投影。

------

## 6. 数据分布偏移

数据生成逻辑由 `SkewProfile` 控制。

其中存储：

- 谓词列
- 主热点值
- 次热点值
- 三级热点值
- 一个 `PredicateShiftMode`
- 每列各自的热点值

### 6.1 热点值的作用

热点值使数据分布刻意倾斜，而非均匀随机。

示例：

- 整数列：`0`、`1`、`7`
- 字符串列：`'hot'`、`'skew'`、`'bias'`
- 浮点列：`0.0`、`1.0`、`3.14`
- 定点数列：`0.00`、`1.00`、`9.99`

### 6.2 `BASELINE` 阶段与 `EXPANSION` 阶段

Oracle3 区分两个生成阶段：

- `DistributionStage.BASELINE`
- `DistributionStage.EXPANSION`

在基线阶段，谓词列的值高度集中于主热点值。

在扩展阶段，Oracle3 应用以下偏移模式之一：

- `PRIMARY_HEAVY`
- `PRIMARY_SECONDARY_SPLIT`
- `SECONDARY_HEAVY`
- `NULL_HEAVY`
- `NOISE_HEAVY`

这意味着 `S2` 不只是"更多行"，而往往是"更多行且具有不同的选择率分布"。

这正是能驱动优化器切换执行计划的关键变化。

------

## 7. 索引策略

Oracle3 刻意创建索引以促进执行计划多样化。

### 7.1 支撑索引

它始终尝试在谓词列上创建单列索引：

```sql
CREATE INDEX i_subset3_x ON table(predicate_col)
```

### 7.2 可选的复合索引

有时 Oracle3 还会创建第二个索引：

```sql
CREATE INDEX i_subset3_c_x ON table(predicate_col, another_col)
```

或：

```sql
CREATE INDEX i_subset3_c_x ON table(another_col, predicate_col)
```

这增加了以下情形的概率：

- `S1` 偏好某条索引路径
- `S2` 偏好另一条索引或全表扫描

------

## 8. 快照与验证内部实现

Oracle3 使用 `QuerySnapshot` 对象存储某一状态下的查询结果。

其中包含：

- `count`
- `rowDigests`（行摘要）
- `maxValues`
- `minValues`
- `plan`

### 8.1 为何使用行摘要

在行集合包含关系的检查中，Oracle3 不直接以 SQL 文本形式比较原始行，而是将每行哈希为摘要。

这样做的优点：

- Java 侧的比较逻辑简单
- 检查结果与查询结果的行顺序无关
- 通过摘要不匹配仍可定位具体缺失的行

### 8.2 行集合包含关系检查

基线快照会捕获 `S1` 的所有行摘要。

之后，Oracle3 在 `S2` 上重新执行相同查询，并从基线集合中移除匹配的摘要：

```text
missing = Res1 的摘要集合
missing -= S2 中出现的摘要
```

若 `missing` 中仍有剩余，则说明：

```text
Res1 不是 Res2 的子集
```

Oracle3 随即报告一个逻辑缺陷。

------

## 9. 已知缺陷抑制

Oracle3 目前包含一项针对性抑制，用于处理此前已发现的 MySQL 缺陷模式，具体涉及：

- 浮点类型谓词列
- `WHERE col = 0.0`
- 某条索引路径返回了错误的 `MAX` 值

若 Oracle3 检测到该精确的已知模式，将跳过本轮，而不再重复报告同一已知缺陷。

该行为由以下参数控制：

```text
-Dsqlancer.subset3.suppressKnownFloatZeroMaxNullBug=true
```

其目的并非整体削弱 Oracle，而是在长时间模糊测试过程中避免反复发现同一已知问题。

------

## 10. 异常处理策略

Oracle3 将结果分为三类：

### 10.1 真实 Oracle 失败

若单调性不变式被违反，Oracle3 抛出：

```text
AssertionError
```

视为候选缺陷。

### 10.2 无效测试轮次

若本轮测试不具备实际意义，Oracle3 以以下异常跳过：

```text
IgnoreMeException
```

典型原因：

- 建表以无害方式失败
- 未找到可用的基线查询
- `S2` 增长量不足
- 连接临时无效

### 10.3 查询或 DML 边界错误

随机生成过程中，许多 MySQL 表达式和插入错误是预期行为。这些错误被加入 `ExpectedErrors`，避免产生误报。

------

## 11. Oracle3 与 Oracle1 发现不同缺陷的原因

Oracle1 主要压测：

- 两张复制表之间的正确性
- 独立物理表下的子集逻辑

Oracle3 主要压测：

- 优化器状态转换过程中的正确性
- 统计信息刷新的影响
- 选择率变化
- 倾斜驱动的计划切换

因此，Oracle3 不只是"另一个子集 Oracle"，其价值在于：

```text
相同的查询
相同的表
相同的逻辑语义
但截然不同的物理执行上下文
```

这使它尤其适合检测 MySQL 在表增长和统计信息更新后切换执行策略时出现的优化器逻辑缺陷。

------

## 12. 当前版本的四项强化措施

相较于最简 Oracle3 设计，当前实现包含以下四项重要强化：

### 12.1 每轮多条基线查询

一次 `S1 -> S2` 状态转换现可测试多条经过验证的查询，而非仅一条。

收益：

- 每轮的缺陷发现密度更高
- 同一表状态转换可覆盖更多执行计划形态

### 12.2 更丰富的谓词模板

Oracle3 不再仅依赖简单的等值谓词。

收益：

- 覆盖更多选择率模式
- 触发更多优化器决策路径

### 12.3 更强的分布偏移

`S1` 刻意设计为规模小且高度集中，而 `S2` 规模更大且通常具有不同的倾斜分布。

收益：

- `ANALYZE TABLE` 后触发计划切换的概率更高

### 12.4 可选的第二索引形态

Oracle3 可在单列谓词索引之外额外创建复合索引。

收益：

- 提供更多竞争性访问路径
- 制造更多优化器不稳定性以供测试

------

## 13. 简化伪代码

```text
创建随机表 T
选定谓词列 P
在 P 上创建索引
可选：创建复合索引
构建倾斜配置

向 T 插入若干热点行
向 T 插入若干倾斜行
向 T 插入若干噪声行
此为 S1

生成若干基线查询 Q1, Q2, ...
对每条 Qi：
    记录 Count1, Max1, Min1, Res1, Plan1

向 T 插入大量倾斜行
提交事务
ANALYZE TABLE T
此为 S2

对每条 Qi：
    记录 Count2, Max2, Min2, Res2, Plan2
    断言 Count1 <= Count2
    断言 Max1 <= Max2
    断言 Min1 >= Min2
    断言 Res1 ⊆ Res2
```

------

## 14. 总结

`MySQLSubsetOracle3` 是一个基于时态状态的子集 Oracle。

其核心逻辑简洁明确：

```text
仅追加方式的数据增长，不应导致单调性查询结果缩减或消失
```

其实际效能来源于以下要素的组合：

- 规模较小的 `S1`
- 规模更大的 `S2`
- 倾斜数据分布
- 通过 `ANALYZE TABLE` 刷新统计信息
- 通过 `EXPLAIN` 进行执行计划对比

因此，Oracle3 可以发现 MySQL 在表增长和统计信息更新后切换执行策略时出现的逻辑缺陷。