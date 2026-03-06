package sqlancer.mysql.oracle;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.gen.MySQLInsertGenerator;
import sqlancer.mysql.gen.MySQLTableGenerator;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.gen.MySQLExpressionGenerator;
// import sqlancer.mysql.gen.MySQLDeleteGenerator;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLJoin;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLTableReference;
import java.util.ArrayList;

/**
 * Subset Oracle — Method 1: Copy + Extra Inserts
 *
 * <p>Constructs S1 ⊆ S2 and verifies aggregate monotonicity:
 * COUNT, MAX, MIN, EXISTS.
 *
 * <p>Verbose output is enabled by default. Disable via system property:
 * {@code -Dsqlancer.subset.verbose=false}
 */
public class MySQLSubsetOracle implements TestOracle<MySQLGlobalState> {

    // ── Verbose flag ─────────────────────────────────────────────────────────
    // Control via:  -Dsqlancer.subset.verbose=false  to silence output
    private static final boolean VERBOSE =
            Boolean.parseBoolean(System.getProperty("sqlancer.subset.verbose", "true"));

    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger(0);

    private final MySQLGlobalState state;
    private final ExpectedErrors insertErrors;
    private String lastQueryString;

    public MySQLSubsetOracle(MySQLGlobalState state) {
        this.state = state;
        this.insertErrors = new ExpectedErrors();
        MySQLErrors.addInsertUpdateErrors(insertErrors);
        MySQLErrors.addExpressionErrors(insertErrors);
    }

    // =========================================================================
    //  Main oracle entry point
    // =========================================================================

    @Override
    public void check() throws Exception {
        int id = TABLE_COUNTER.incrementAndGet();
        String s1Name = "s1_sub_" + id;
        String s2Name = "s2_sub_" + id;

        log("╔══════════════════════════════════════════════════════════════");
        log("║  SUBSET ORACLE  round #" + id);
        log("╚══════════════════════════════════════════════════════════════");

        try {
            // ── Step 1: Create S1 with a random schema ────────────────────
            log("\n[Step 1] Creating S1: " + s1Name);
            SQLQueryAdapter createS1 = MySQLTableGenerator.generate(state, s1Name);
            logSQL(createS1.getQueryString());
            boolean created = state.executeStatement(createS1);
            if (!created) {
                log("  ✗ CREATE TABLE failed — skipping round");
                throw new IgnoreMeException();
            }
            state.updateSchema();

            MySQLTable s1Table = findTable(s1Name);
            if (s1Table == null) {
                log("  ✗ Table not found in schema after creation — skipping round");
                throw new IgnoreMeException();
            }

            List<String> colNames = s1Table.getColumns().stream()
                    .map(MySQLColumn::getName).collect(Collectors.toList());
            log("  ✓ Created with columns: " + colNames);

            // ── Step 2: Populate S1 with random data ──────────────────────
            log("\n[Step 2] Inserting random rows into S1...");
            MySQLExpressionGenerator genS1 = new MySQLExpressionGenerator(state).setColumns(s1Table.getColumns());
            int nrS1Ops = 10 + Randomly.smallNumber();
            for (int i = 0; i < nrS1Ops; i++) {
                try {
                    SQLQueryAdapter op;
                    double p = Randomly.getPercentage();
                    if (p < 0.6) {
                        // 60%: INSERT 随机数据
                        op = MySQLInsertGenerator.insertRow(state, s1Table);
                    } else if (p < 0.8) {
                        // 20%: DELETE 部分行（直接构造，精确针对 s1）
                        String deleteSql = "DELETE FROM " + s1Name + " WHERE "
                                + MySQLVisitor.asString(genS1.generateExpression());
                        ExpectedErrors delErrors = new ExpectedErrors();
                        MySQLErrors.addExpressionErrors(delErrors);
                        op = new SQLQueryAdapter(deleteSql, delErrors);
                    } else {
                        // 20%: UPDATE 部分行（直接构造，精确针对 s1）
                        MySQLColumn targetCol = Randomly.fromList(s1Table.getColumns());
                        String updateSql = "UPDATE " + s1Name
                                + " SET `" + targetCol.getName() + "` = "
                                + MySQLVisitor.asString(genS1.generateConstant())
                                + " WHERE " + MySQLVisitor.asString(genS1.generateExpression());
                        ExpectedErrors updErrors = new ExpectedErrors();
                        MySQLErrors.addInsertUpdateErrors(updErrors);
                        MySQLErrors.addExpressionErrors(updErrors);
                        op = new SQLQueryAdapter(updateSql, updErrors);
                    }
                    logSQL(op.getQueryString());
                    state.executeStatement(op);
                } catch (Throwable e) {
                    log("  ⚠ Operation skipped: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                }
            }
            log("  Inserting boundary/null noise rows into S1...");
            insertNoiseRows(s1Table);
            Long countS1 = executeSingleLong("SELECT COUNT(*) FROM " + s1Name);
            log("  ✓ S1 now has " + countS1 + " row(s) (random + boundary + null values)");

            // ── Step 3: Create S2 (same schema) and copy S1 ──────────────
            log("\n[Step 3] Creating S2 with same schema, then copying S1 → S2...");
            String createS2Sql = "CREATE TABLE " + s2Name + " LIKE " + s1Name;
            logSQL(createS2Sql);
            state.executeStatement(new SQLQueryAdapter(createS2Sql, true));

            String copySQL = "INSERT INTO " + s2Name + " SELECT * FROM " + s1Name;
            logSQL(copySQL);
            state.executeStatement(new SQLQueryAdapter(copySQL, insertErrors));

            Long countS2AfterCopy = executeSingleLong("SELECT COUNT(*) FROM " + s2Name);
            log("  S1 rows: " + countS1 + "  →  S2 rows after copy: " + countS2AfterCopy);

            if (countS1 == null || countS2AfterCopy == null
                    || !countS1.equals(countS2AfterCopy)) {
                log("  ✗ Copy incomplete (e.g. UNIQUE constraint rejected rows) — skipping");
                log("    This is NOT a bug; the subset invariant cannot be guaranteed here.");
                throw new IgnoreMeException();
            }
            log("  ✓ All " + countS1 + " row(s) copied → S1 ⊆ S2 established");

            state.updateSchema();
            MySQLTable s2Table = findTable(s2Name);
            if (s2Table == null) {
                throw new IgnoreMeException();
            }

            // ── Step 4: Insert extra random rows into S2 only ─────────────
            log("\n[Step 4] Inserting extra rows into S2 only (S1 unchanged)...");
            int nrS2Extra = 1 + Randomly.smallNumber();
            for (int i = 0; i < nrS2Extra; i++) {
                try {
                    SQLQueryAdapter ins = MySQLInsertGenerator.insertRow(state, s2Table);
                    logSQL(ins.getQueryString());
                    state.executeStatement(ins);
                } catch (Exception e) {
                    log("  ⚠ INSERT skipped: " + e.getMessage());
                }
            }
            Long countS2Final = executeSingleLong("SELECT COUNT(*) FROM " + s2Name);
            log("  ✓ Final state → S1: " + countS1 + " rows  |  S2: " + countS2Final + " rows");
            log("  Invariant confirmed: " + s1Name + " ⊆ " + s2Name);

            // ── Step 5: Verify aggregate monotonicity properties ──────────
            log("\n[Step 5] Checking aggregate monotonicity...");

            verifyCount(s1Name, s2Name, countS1, countS2Final);
            verifyExists(s1Name, s2Name, countS1, countS2Final);

            List<MySQLColumn> numericCols = s1Table.getColumns().stream()
                    .filter(c -> c.getType().isNumeric())
                    .collect(Collectors.toList());

            if (numericCols.isEmpty()) {
                log("  ℹ No numeric columns — MAX/MIN checks skipped");
            }
            for (MySQLColumn col : numericCols) {
                verifyMax(s1Name, s2Name, col.getName());
                verifyMin(s1Name, s2Name, col.getName());
            }

            verifySelectSubset(s1Name, s2Name, s1Table);
            
            log("\n  ✓✓✓ All checks PASSED for round #" + id);

        } finally {
            log("\n[Cleanup] Dropping temporary tables " + s1Name + ", " + s2Name + "...");
            dropIfExists(s2Name);
            dropIfExists(s1Name);
            try { state.updateSchema(); } catch (Exception ignored) { }
            log("──────────────────────────────────────────────────────────────\n");
        }
    }

    // =========================================================================
    //  Property checkers
    // =========================================================================

    /**
     * COUNT(*) monotonicity: COUNT(S1) ≤ COUNT(S2).
     */
    private void verifyCount(String s1Name, String s2Name,
            Long knownC1, Long knownC2) throws Exception {
        String q1 = "SELECT COUNT(*) FROM " + s1Name;
        String q2 = "SELECT COUNT(*) FROM " + s2Name;
        Long c1 = (knownC1 != null) ? knownC1 : executeSingleLong(q1);
        Long c2 = (knownC2 != null) ? knownC2 : executeSingleLong(q2);

        String status;
        if (c1 == null || c2 == null) {
            status = "? SKIP (null result)";
        } else {
            status = (c1 <= c2) ? "✓ PASS" : "✗ FAIL  ← BUG DETECTED";
        }
        log(String.format("  COUNT(*)  →  %s=%-6s  ≤  %s=%-6s   [%s]",
                s1Name, c1, s2Name, c2, status));

        if (c1 != null && c2 != null && c1 > c2) {
            lastQueryString = q1 + "\n" + q2;
            throw new AssertionError(String.format(
                    "COUNT(*) subset violation: COUNT(%s)=%d > COUNT(%s)=%d\n  Q1: %s\n  Q2: %s",
                    s1Name, c1, s2Name, c2, q1, q2));
        }
    }

    /**
     * EXISTS monotonicity: EXISTS(S1) ⟹ EXISTS(S2).
     */
    private void verifyExists(String s1Name, String s2Name,
            Long knownC1, Long knownC2) throws Exception {
        boolean e1 = (knownC1 != null) ? knownC1 > 0
                : Long.valueOf(1L).equals(executeSingleLong(
                        "SELECT EXISTS(SELECT 1 FROM " + s1Name + " LIMIT 1)"));
        boolean e2 = (knownC2 != null) ? knownC2 > 0
                : Long.valueOf(1L).equals(executeSingleLong(
                        "SELECT EXISTS(SELECT 1 FROM " + s2Name + " LIMIT 1)"));

        String status = (!e1 || e2) ? "✓ PASS" : "✗ FAIL  ← BUG DETECTED";
        log(String.format("  EXISTS     →  %s=%-5s  ⟹  %s=%-5s   [%s]",
                s1Name, e1, s2Name, e2, status));

        if (e1 && !e2) {
            throw new AssertionError(String.format(
                    "EXISTS subset violation: %s is non-empty but %s is empty",
                    s1Name, s2Name));
        }
    }

    /**
     * MAX monotonicity: MAX(S1.col) ≤ MAX(S2.col).
     */
    private void verifyMax(String s1Name, String s2Name, String col) throws Exception {
        String q1 = "SELECT MAX(`" + col + "`) FROM " + s1Name;
        String q2 = "SELECT MAX(`" + col + "`) FROM " + s2Name;
        Double max1 = executeSingleDouble(q1);
        Double max2 = executeSingleDouble(q2);

        if (max1 == null) {
            log(String.format("  MAX(%-6s) →  S1=NULL (empty table)   [✓ PASS vacuously]", col));
            return;
        }
        boolean pass = (max2 != null) && (max1 <= max2 + 1e-9);
        log(String.format("  MAX(%-6s) →  %s=%-12s  ≤  %s=%-12s   [%s]",
                col, s1Name, max1, s2Name, max2, pass ? "✓ PASS" : "✗ FAIL  ← BUG DETECTED"));
        if (!pass) {
            lastQueryString = q1 + "\n" + q2;
            throw new AssertionError(String.format(
                    "MAX(%s) subset violation: MAX(%s)=%s > MAX(%s)=%s\n  Q1: %s\n  Q2: %s",
                    col, s1Name, max1, s2Name, max2, q1, q2));
        }
    }

    /**
     * MIN anti-monotonicity: MIN(S1.col) ≥ MIN(S2.col).
     */
    private void verifyMin(String s1Name, String s2Name, String col) throws Exception {
        String q1 = "SELECT MIN(`" + col + "`) FROM " + s1Name;
        String q2 = "SELECT MIN(`" + col + "`) FROM " + s2Name;
        Double min1 = executeSingleDouble(q1);
        Double min2 = executeSingleDouble(q2);

        if (min1 == null) {
            log(String.format("  MIN(%-6s) →  S1=NULL (empty table)   [✓ PASS vacuously]", col));
            return;
        }
        boolean pass = (min2 != null) && (min2 <= min1 + 1e-9);
        log(String.format("  MIN(%-6s) →  %s=%-12s  ≥  %s=%-12s   [%s]",
                col, s1Name, min1, s2Name, min2, pass ? "✓ PASS" : "✗ FAIL  ← BUG DETECTED"));
        if (!pass) {
            lastQueryString = q1 + "\n" + q2;
            throw new AssertionError(String.format(
                    "MIN(%s) subset violation: MIN(%s)=%s < MIN(%s)=%s\n  Q1: %s\n  Q2: %s",
                    col, s1Name, min1, s2Name, min2, q1, q2));
        }
    }

    private void verifySelectSubset(String s1Name, String s2Name, MySQLTable s1Table) throws Exception {
        int nrChecks = 3 + Randomly.smallNumber();
        log("\n[Step 6] Random SELECT subset checks (" + nrChecks + " queries)...");

        // MySQLExpressionGenerator gen = new MySQLExpressionGenerator(state)
        //         .setColumns(s1Table.getColumns());
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);

        for (int i = 0; i < nrChecks; i++) {
            // log("  [SELECT#" + (i + 1) + "] Starting iteration...");
            // 获取可用的全局表（排除 s1 和 s2 自身）用于 JOIN
            List<MySQLTable> globalTables = state.getSchema().getDatabaseTables().stream()
                    .filter(t -> !t.getName().equalsIgnoreCase(s1Name)
                            && !t.getName().equalsIgnoreCase(s2Name))
                    .collect(Collectors.toList());

            // 决定是否加入 JOIN，以及 JOIN 哪张全局表
            MySQLTable joinTable = (!globalTables.isEmpty() && Randomly.getBoolean())
                    ? Randomly.fromList(globalTables) : null;

            // 表达式生成器：s1 的列 + 可能的 JOIN 表的列
            List<MySQLColumn> allCols = new ArrayList<>(s1Table.getColumns());
            if (joinTable != null) {
                allCols.addAll(joinTable.getColumns());
            }
            MySQLExpressionGenerator selGen = new MySQLExpressionGenerator(state).setColumns(allCols);

            // 构建 MySQLSelect 对象
            MySQLSelect sel = new MySQLSelect();
            sel.setSelectType(Randomly.fromOptions(MySQLSelect.SelectType.values()));

            // fetch 列：只取 s1Table 的列（用 MySQLColumnReference 携带表信息，
            // asString() 会输出 s1_sub_N.col，后续整体替换表名即可得到 q2）
            List<MySQLExpression> fetchCols = s1Table.getColumns().stream()
                    .map(c -> new MySQLColumnReference(c, null))
                    .collect(Collectors.toList());
            sel.setFetchColumns(fetchCols);

            // FROM：s1Table（q2 由字符串替换得到）
            sel.setFromList(List.of(new MySQLTableReference(s1Table)));

            // 可选 WHERE
            if (Randomly.getBoolean()) {
                sel.setWhereClause(selGen.generateExpression());
            }

            // 可选 GROUP BY + HAVING
            if (Randomly.getBoolean()) {
                List<MySQLExpression> groupByCols = s1Table.getColumns().stream()
                        .map(c -> new MySQLColumnReference(c, null))
                        .collect(Collectors.toList());
                sel.setGroupByExpressions(groupByCols);
                if (Randomly.getBoolean()) {
                    sel.setHavingClause(selGen.generateExpression());
                }
            }

            // 可选 JOIN：只允许 INNER JOIN 和 LEFT JOIN（s1/s2 始终作为左表）
            // RIGHT JOIN 会破坏子集关系，故排除
            if (joinTable != null) {
                MySQLJoin.JoinType joinType = Randomly.fromOptions(
                        MySQLJoin.JoinType.INNER, MySQLJoin.JoinType.LEFT);
                MySQLExpression onClause = selGen.generateExpression();
                MySQLJoin join = new MySQLJoin(joinTable, onClause, joinType);
                sel.setJoinList(List.of(join));
            }

            // 不加 LIMIT / OFFSET：会截断结果集破坏子集关系

            String q1, q2;
            try {
                q1 = MySQLVisitor.asString(sel);
                // 把所有 s1Name 替换成 s2Name，得到等价的 S2 查询
                // s1_sub_N 足够唯一，不会误替换 joinTable 的列名
                q2 = q1.replace(s1Name, s2Name);
            } catch (Throwable e) {
                log("  ⚠ Could not generate SELECT — skipping: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                continue;
            }
            logSQL(q1);
            logSQL(q2);

            java.util.Set<String> rows1, rows2;
            try {
                int colCount = s1Table.getColumns().size();
                rows1 = executeAndGetRowSet(q1, colCount);
                rows2 = executeAndGetRowSet(q2, colCount);
            } catch (Exception e) {
                log("  ⚠ Query failed (" + e.getMessage() + ") — skipping this check");
                continue;
            }

            // 检查 rows1 ⊆ rows2
            java.util.Set<String> missing = new java.util.LinkedHashSet<>(rows1);
            missing.removeAll(rows2);

            String queryDisplay = q1.length() > 60 ? q1.substring(0, 57) + "..." : q1;
            boolean pass = missing.isEmpty();
            log(String.format("  SELECT#%d  →  |S1|=%d  |S2|=%d  missing=%d   [%s]%n    Q: %s",
                    i + 1, rows1.size(), rows2.size(), missing.size(),
                    pass ? "✓ PASS" : "✗ FAIL  ← BUG DETECTED", queryDisplay));

            if (!pass) {
                lastQueryString = q1 + "\n" + q2;
                throw new AssertionError(String.format(
                        "SELECT subset violation: result(%s WHERE ...) ⊄ result(%s WHERE ...)\n"
                        + "  Missing rows: %s\n  Q1: %s\n  Q2: %s",
                        s1Name, s2Name, missing, q1, q2));
            }
        }
    }
    /**
     * 向指定表插入噪音值，噪音值包括各类型的边界值和 NULL。
     * 每次调用会尝试插入多行，每行对每一列独立选取一个噪音值。
     * 因约束冲突（UNIQUE、NOT NULL 等）导致的失败直接忽略。
     */
    private void insertNoiseRows(MySQLTable table) {
        List<MySQLColumn> cols = table.getColumns();
        // 每列的边界值列表（字符串形式，直接拼入 SQL）
        java.util.Map<MySQLColumn, List<String>> boundaryMap = new java.util.LinkedHashMap<>();
        for (MySQLColumn col : cols) {
            List<String> vals = new java.util.ArrayList<>();
            switch (col.getType()) {
                case INT:
                    vals.add("0");
                    vals.add("1");
                    vals.add("-1");
                    vals.add("-128");           // TINYINT MIN
                    vals.add("127");            // TINYINT MAX
                    vals.add("-32768");         // SMALLINT MIN
                    vals.add("32767");          // SMALLINT MAX
                    vals.add("-2147483648");    // INT MIN
                    vals.add("2147483647");     // INT MAX
                    vals.add("-9223372036854775808"); // BIGINT MIN
                    vals.add("9223372036854775807");  // BIGINT MAX
                    vals.add("NULL");
                    break;
                case VARCHAR:
                    vals.add("''");            // 空字符串
                    vals.add("' '");           // 空格
                    vals.add("'a'");
                    // 500字符长字符串（匹配 VARCHAR(500)）
                    vals.add("'" + "a".repeat(500) + "'");
                    vals.add("'%'");           // LIKE 通配符
                    vals.add("'_'");           // LIKE 单字符通配符
                    vals.add("'NULL'");        // 字符串"NULL"
                    vals.add("'0'");
                    vals.add("NULL");
                    break;
                case FLOAT:
                case DOUBLE:
                    vals.add("0");
                    vals.add("0.0");
                    vals.add("-0.0");
                    vals.add("1.0");
                    vals.add("-1.0");
                    vals.add("3.4028235E38");   // FLOAT MAX 近似
                    vals.add("-3.4028235E38");
                    vals.add("1.7976931348623157E308");  // DOUBLE MAX 近似
                    vals.add("1.4E-45");        // 接近 0 的最小正值
                    vals.add("NULL");
                    break;
                case DECIMAL:
                    vals.add("0");
                    vals.add("0.00");
                    vals.add("1.00");
                    vals.add("-1.00");
                    vals.add("99999999999999999999999999999999999.99"); // 超大值（会被截断但不崩溃）
                    vals.add("-99999999999999999999999999999999999.99");
                    vals.add("NULL");
                    break;
                default:
                    vals.add("NULL");
                    break;
            }
            boundaryMap.put(col, vals);
        }

        // 对每列的边界值逐一尝试插入（每次一行，专门测试该列边界，其他列用 NULL）
        for (MySQLColumn targetCol : cols) {
            for (String boundaryVal : boundaryMap.get(targetCol)) {
                StringBuilder sb = new StringBuilder();
                sb.append("INSERT INTO ").append(table.getName()).append(" (");
                sb.append(cols.stream().map(MySQLColumn::getName)
                        .collect(Collectors.joining(", ")));
                sb.append(") VALUES (");
                for (int i = 0; i < cols.size(); i++) {
                    if (i > 0) sb.append(", ");
                    if (cols.get(i).equals(targetCol)) {
                        sb.append(boundaryVal);
                    } else {
                        sb.append("NULL");
                    }
                }
                sb.append(")");
                try {
                    logSQL(sb.toString());
                    state.executeStatement(new SQLQueryAdapter(sb.toString(), insertErrors));
                } catch (Throwable e) {
                    log("  ⚠ Noise INSERT skipped: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                }
            }
        }

        // 额外插入一行全 NULL
        StringBuilder nullRow = new StringBuilder();
        nullRow.append("INSERT INTO ").append(table.getName()).append(" (");
        nullRow.append(cols.stream().map(MySQLColumn::getName).collect(Collectors.joining(", ")));
        nullRow.append(") VALUES (");
        nullRow.append(cols.stream().map(c -> "NULL").collect(Collectors.joining(", ")));
        nullRow.append(")");
        try {
            logSQL(nullRow.toString());
            state.executeStatement(new SQLQueryAdapter(nullRow.toString(), insertErrors));
        } catch (Throwable e) {
            log("  ⚠ All-NULL row skipped: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    // =========================================================================
    //  Utility helpers
    // =========================================================================

    private MySQLTable findTable(String name) {
        return state.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getName().equalsIgnoreCase(name))
                .findFirst().orElse(null);
    }

    private void dropIfExists(String tableName) {
        try {
            state.executeStatement(
                    new SQLQueryAdapter("DROP TABLE IF EXISTS " + tableName, true));
        } catch (Exception ignored) {
        }
    }

    private Long executeSingleLong(String query) throws SQLException {
        lastQueryString = query;
        SQLancerResultSet rs = new SQLQueryAdapter(query, new ExpectedErrors())
                .executeAndGet(state);
        if (rs == null) {
            return null;
        }
        try {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } finally {
            rs.close();
        }
        return null;
    }

    private Double executeSingleDouble(String query) throws SQLException {
        lastQueryString = query;
        SQLancerResultSet rs = new SQLQueryAdapter(query, new ExpectedErrors())
                .executeAndGet(state);
        if (rs == null) {
            return null;
        }
        try {
            if (rs.next()) {
                String val = rs.getString(1);
                if (val == null) {
                    return null;
                }
                try {
                    return Double.parseDouble(val);
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        } finally {
            rs.close();
        }
        return null;
    }

    private java.util.Set<String> executeAndGetRowSet(String query, int colCount) throws SQLException {
        java.util.Set<String> rows = new java.util.LinkedHashSet<>();
        SQLancerResultSet rs = new SQLQueryAdapter(query, new ExpectedErrors())
                .executeAndGet(state);
        if (rs == null) return rows;
        try {
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= colCount; i++) {
                    if (i > 1) row.append("|");
                    row.append(rs.getString(i));
                }
                rows.add(row.toString());
            }
        } finally {
            rs.close();
        }
        return rows;
    }

    // =========================================================================
    //  Logging helpers
    // =========================================================================

    private static void log(String msg) {
        if (VERBOSE) {
            System.out.println(msg);
        }
    }

    /** Prints an SQL statement, truncating long strings for readability. */
    private static void logSQL(String sql) {
        if (!VERBOSE) {
            return;
        }
        String trimmed = (sql == null) ? "(null)" : sql.trim().replace("\n", " ");
        if (trimmed.length() > 120) {
            trimmed = trimmed.substring(0, 117) + "...";
        }
        System.out.println("  SQL> " + trimmed);
    }

    // =========================================================================
    //  TestOracle interface
    // =========================================================================

    @Override
    public Reproducer<MySQLGlobalState> getLastReproducer() {
        return null;
    }

    @Override
    public String getLastQueryString() {
        return lastQueryString;
    }
}