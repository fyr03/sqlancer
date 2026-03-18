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
 * Subset Oracle — Method 2: Random Row Sampling
 *
 * <p>Constructs S2 ⊆ S1 by iterating over every row of S1 and inserting it
 * into S2 with 50 % probability. Verifies aggregate monotonicity:
 * COUNT, MAX, MIN, EXISTS (with S2 as the smaller set).
 *
 * <p>Verbose output is enabled by default. Disable via system property:
 * {@code -Dsqlancer.subset.verbose=false}
 */
public class MySQLSubsetOracle2 implements TestOracle<MySQLGlobalState> {

    // ── Verbose flag ─────────────────────────────────────────────────────────
    // Control via:  -Dsqlancer.subset.verbose=false  to silence output
    private static final boolean VERBOSE =
            Boolean.parseBoolean(System.getProperty("sqlancer.subset.verbose", "true"));

    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger(0);

    private final MySQLGlobalState state;
    private final ExpectedErrors insertErrors;
    private String lastQueryString;

    public MySQLSubsetOracle2(MySQLGlobalState state) {
        this.state = state;
        this.insertErrors = new ExpectedErrors();
        MySQLErrors.addInsertUpdateErrors(insertErrors);
        MySQLErrors.addExpressionErrors(insertErrors);
        // 字符串插入数值列
        insertErrors.add("Incorrect string value");
        insertErrors.add("Incorrect double value");
        insertErrors.add("Incorrect integer value");
        insertErrors.add("Incorrect decimal value");
        // 多字节字符集问题
        insertErrors.add("Incorrect string value");
        insertErrors.add("Invalid utf8");
        insertErrors.add("Cannot convert");
        insertErrors.add("is not valid for CHARACTER SET");
        // REPLACE 特有
        insertErrors.add("doesn't have this option");
        insertErrors.add("DELAYED option");
    }

    // =========================================================================
    //  Main oracle entry point
    // =========================================================================

    @Override
    public void check() throws Exception {
        int id = TABLE_COUNTER.incrementAndGet();
        String s1Name = "s1_sub2_" + id;
        String s2Name = "s2_sub2_" + id;

        log("╔══════════════════════════════════════════════════════════════");
        log("║  SUBSET ORACLE 2  round #" + id);
        log("╚══════════════════════════════════════════════════════════════");

        try {
            // ── Step 1: Create S1 with a random schema ────────────────────
            try { state.updateSchema(); } catch (Exception ignored) { }
            log("\n[Step 1] Creating S1: " + s1Name);
            // SQLQueryAdapter createS1 = MySQLTableGenerator.generate(state, s1Name);
            SQLQueryAdapter createS1;
            do {
                createS1 = MySQLTableGenerator.generate(state, s1Name);
            } while (createS1.getQueryString().toUpperCase().contains(" LIKE "));
            logSQL(createS1.getQueryString());

            // boolean created = state.executeStatement(createS1);
            // if (!created) {
            //     log("  ✗ CREATE TABLE failed — skipping round");
            //     throw new IgnoreMeException();
            // }
            state.executeStatement(createS1);
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
            int nrS1Ops = 200 + Randomly.smallNumber();
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

            // ── Step 3: Create S2 (same schema), then sample rows from S1 ─
            log("\n[Step 3] Creating S2 with same schema, then sampling S1 → S2 (50% per row)...");
            String createS2Sql = "CREATE TABLE " + s2Name + " LIKE " + s1Name;
            logSQL(createS2Sql);
            state.executeStatement(new SQLQueryAdapter(createS2Sql, true));

            // SQL-level random sampling: ~50% of S1 rows via RAND()
            long randSeed = state.getRandomly().getLong(0, Integer.MAX_VALUE);
            String sampleSQL = "INSERT IGNORE INTO " + s2Name
                    + " SELECT * FROM " + s1Name
                    + " WHERE RAND(" + randSeed + ") < 0.5";
            logSQL(sampleSQL);
            try {
                state.executeStatement(new SQLQueryAdapter(sampleSQL, insertErrors));
            } catch (Throwable e) {
                log("  ⚠ Sampling INSERT skipped: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }

            Long countS2Final = executeSingleLong("SELECT COUNT(*) FROM " + s2Name);
            log("  ✓ Final state → S1: " + countS1 + " rows  |  S2: " + countS2Final + " rows");
            log("  Invariant established: " + s2Name + " ⊆ " + s1Name);

            state.updateSchema();
            MySQLTable s2Table = findTable(s2Name);
            if (s2Table == null) {
                throw new IgnoreMeException();
            }

            // ── Step 4: Verify aggregate monotonicity properties ──────────
            // NOTE: S2 ⊆ S1, so arguments are swapped compared to Method 1:
            //   the "subset" argument is s2, and the "superset" argument is s1.
            log("\n[Step 4] Checking aggregate monotonicity (S2 ⊆ S1)...");

            verifyCount(s2Name, s1Name, countS2Final, countS1, s2Table);
            verifyExists(s2Name, s1Name, countS2Final, countS1);

            List<MySQLColumn> numericCols = s2Table.getColumns().stream()
                    .filter(c -> c.getType().isNumeric())
                    .collect(Collectors.toList());

            if (numericCols.isEmpty()) {
                log("  ℹ No numeric columns — MAX/MIN checks skipped");
            }
            for (MySQLColumn col : numericCols) {
                verifyMax(s2Name, s1Name, col.getName(), s2Table);
                verifyMin(s2Name, s1Name, col.getName(), s2Table);
            }

            for (MySQLColumn col : numericCols) {
                verifyCountDistinct(s2Name, s1Name, col.getName());
            }
            verifySelectSubset(s2Name, s1Name, s2Table);
            verifyInSubquery(s2Name, s1Name, s2Table);

            log("\n  ✓✓✓ All checks PASSED for round #" + id);

        } catch (java.sql.SQLNonTransientConnectionException e) {
            // 连接被 MySQL 强制断开，跳过本轮，不算 bug
            throw new IgnoreMeException();
        } catch (com.mysql.cj.jdbc.exceptions.CommunicationsException e) {
            throw new IgnoreMeException();
        } catch (java.sql.SQLRecoverableException e) {
            throw new IgnoreMeException();
        } catch (java.sql.SQLException e) {
            if (e.getMessage() != null && e.getMessage().contains("NumberFormatException")) {
                throw new IgnoreMeException(); // MySQL bug114533: 畸形浮点数字符串
            }
            throw e;
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
    //  Convention: s1Name = the SUBSET, s2Name = the SUPERSET.
    // =========================================================================

    /**
     * COUNT(*) monotonicity: COUNT(subset) ≤ COUNT(superset).
     */
    private void verifyCount(String s1Name, String s2Name,
            Long knownC1, Long knownC2, MySQLTable s1Table) throws Exception {
        // 随机决定是否加 WHERE 条件
        String where = "";
        if (Randomly.getBoolean()) {
            MySQLExpressionGenerator gen = new MySQLExpressionGenerator(state)
                    .setColumns(s1Table.getColumns());
            // 生成一次，Q1 和 Q2 共用同一个字符串
            where = " WHERE " + MySQLVisitor.asString(gen.generateExpression());
        }
        String q1 = "SELECT COUNT(*) FROM " + s1Name + where;
        String q2 = q1.replaceAll("\\b" + s1Name + "\\b", s2Name);
        Long c1, c2;
        if (where.isEmpty()) {
            c1 = (knownC1 != null) ? knownC1 : executeSingleLong(q1);
            c2 = (knownC2 != null) ? knownC2 : executeSingleLong(q2);
        } else {
            c1 = executeSingleLong(q1);
            c2 = executeSingleLong(q2);
        }

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
     * EXISTS monotonicity: EXISTS(subset) ⟹ EXISTS(superset).
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
     * MAX monotonicity: MAX(subset.col) ≤ MAX(superset.col).
     */
    private void verifyMax(String s1Name, String s2Name, String col, MySQLTable s1Table) throws Exception {
        // 随机决定是否加 WHERE 条件
        String where = "";
        if (Randomly.getBoolean()) {
            MySQLExpressionGenerator gen = new MySQLExpressionGenerator(state)
                    .setColumns(s1Table.getColumns());
            // 生成一次，Q1 和 Q2 共用同一个字符串
            where = " WHERE " + MySQLVisitor.asString(gen.generateExpression());
        }
        String q1 = "SELECT MAX(`" + col + "`) FROM " + s1Name + where;
        String q2 = q1.replaceAll("\\b" + s1Name + "\\b", s2Name);
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
     * MIN anti-monotonicity: MIN(subset.col) ≥ MIN(superset.col).
     */
    private void verifyMin(String s1Name, String s2Name, String col, MySQLTable s1Table) throws Exception {
        // 随机决定是否加 WHERE 条件
        String where = "";
        if (Randomly.getBoolean()) {
            MySQLExpressionGenerator gen = new MySQLExpressionGenerator(state)
                    .setColumns(s1Table.getColumns());
            // 生成一次，Q1 和 Q2 共用同一个字符串
            where = " WHERE " + MySQLVisitor.asString(gen.generateExpression());
        }
        String q1 = "SELECT MIN(`" + col + "`) FROM " + s1Name + where;
        String q2 = q1.replaceAll("\\b" + s1Name + "\\b", s2Name);
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
        int nrChecks = 15 + Randomly.smallNumber();
        log("\n[Step 5] Random SELECT subset checks (" + nrChecks + " queries)...");

        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);

        for (int i = 0; i < nrChecks; i++) {
            try {
                List<MySQLTable> globalTables = state.getSchema().getDatabaseTables().stream()
                        .filter(t -> !t.getName().equalsIgnoreCase(s1Name)
                                && !t.getName().equalsIgnoreCase(s2Name))
                        .collect(Collectors.toList());

                MySQLTable joinTable = (!globalTables.isEmpty() && Randomly.getBoolean())
                        ? Randomly.fromList(globalTables) : null;

                List<MySQLColumn> allCols = new ArrayList<>(s1Table.getColumns());
                if (joinTable != null) {
                    allCols.addAll(joinTable.getColumns());
                }

                MySQLExpressionGenerator selGen = new MySQLExpressionGenerator(state);
                selGen.setTablesAndColumns(new sqlancer.mysql.MySQLSchema.MySQLTables(
                        joinTable != null ? List.of(s1Table, joinTable) : List.of(s1Table)));

                MySQLSelect sel = new MySQLSelect();
                sel.setSelectType(Randomly.fromOptions(MySQLSelect.SelectType.values()));
                List<MySQLExpression> fetchCols = s1Table.getColumns().stream()
                        .map(c -> new MySQLColumnReference(c, null))
                        .collect(Collectors.toList());
                sel.setFetchColumns(fetchCols);
                sel.setFromList(List.of(new MySQLTableReference(s1Table)));
                if (Randomly.getBoolean()) {
                    sel.setWhereClause(selGen.generateExpression());
                }
                // if (Randomly.getBoolean()) {
                //     List<MySQLExpression> groupByCols = s1Table.getColumns().stream()
                //             .filter(c -> {  // ← 只对非文本列做 GROUP BY
                //                 String typeName = c.getType().name().toUpperCase();
                //                 return !typeName.contains("TEXT") && !typeName.contains("BLOB")
                //                     && !typeName.contains("CHAR") && !typeName.contains("BINARY");
                //             })
                //             .map(c -> new MySQLColumnReference(c, null))
                //             .collect(Collectors.toList());
                //     if (!groupByCols.isEmpty()) {
                //         sel.setGroupByExpressions(groupByCols);
                //     }
                // }
                if (joinTable != null) {
                    MySQLJoin.JoinType joinType = Randomly.fromOptions(
                            MySQLJoin.JoinType.INNER, MySQLJoin.JoinType.LEFT);
                    MySQLExpression onClause = selGen.generateExpression();
                    MySQLJoin join = new MySQLJoin(joinTable, onClause, joinType);
                    sel.setJoinList(List.of(join));
                }

                // q1 is built on s1Table (the subset). Replace s1Name with s2Name to get q2 (superset).
                String q1 = MySQLVisitor.asString(sel);
                String q2 = q1.replaceAll("\\b" + s1Name + "\\b", s2Name);
                logSQL(q1);
                logSQL(q2);

                java.util.Set<String> rows1 = java.util.Collections.emptySet();
                java.util.Set<String> rows2 = java.util.Collections.emptySet();
                state.executeStatement(new SQLQueryAdapter(
                    "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", true));
                state.executeStatement(new SQLQueryAdapter("ROLLBACK", true));
                state.executeStatement(new SQLQueryAdapter("START TRANSACTION", true));
                boolean queryFailed = false;
                try {
                    rows1 = executeAndGetRowSet(q1, s1Table.getColumns().size());
                    rows2 = executeAndGetRowSet(q2, s1Table.getColumns().size());
                } catch (SQLException e) {
                    // Q1 或 Q2 执行失败（例如算术溢出 ERROR 1690）
                    // 这不是 MySQL 的子集语义 bug，直接跳过本次检查
                    log("  ⚠ SELECT#" + (i + 1) + " skipped (query execution error): "
                            + e.getClass().getSimpleName() + ": " + e.getMessage());
                    queryFailed = true;   // ← 标记失败
                } finally {
                    state.executeStatement(new SQLQueryAdapter("ROLLBACK", true));
                }
                if (queryFailed) continue;   // ← 跳过后续比较，进入下一次循环
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
                            "SELECT subset violation: result(%s) ⊄ result(%s)\n"
                            + "  Missing rows: %s\n  Q1: %s\n  Q2: %s",
                            s1Name, s2Name, missing, q1, q2));
                }
            } catch (AssertionError e) {
                throw e;
            } catch (java.sql.SQLNonTransientConnectionException 
                | java.sql.SQLRecoverableException e) {
                // 连接断开，不能继续，向外抛让外层处理
                throw e;
            } catch (Throwable e) {
                log("  ⚠ SELECT#" + (i + 1) + " skipped: "
                        + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
    }

    private void verifyCountDistinct(String s1Name, String s2Name, String col) throws Exception {
        String q1 = "SELECT COUNT(DISTINCT `" + col + "`) FROM " + s1Name;
        String q2 = "SELECT COUNT(DISTINCT `" + col + "`) FROM " + s2Name;
        Long c1 = executeSingleLong(q1);
        Long c2 = executeSingleLong(q2);
        if (c1 != null && c2 != null && c1 > c2) {
            throw new AssertionError(String.format(
                "COUNT(DISTINCT %s) violation: S1=%d > S2=%d\n  Q1: %s\n  Q2: %s",
                col, c1, c2, q1, q2));
        }
    }

    private void verifyInSubquery(String s1Name, String s2Name, MySQLTable s1Table) throws Exception {
        // 用全局表做驱动，测 IN 子查询
        List<MySQLTable> globalTables = state.getSchema().getDatabaseTables().stream()
            .filter(t -> !t.getName().equalsIgnoreCase(s1Name)
                    && !t.getName().equalsIgnoreCase(s2Name))
            .collect(Collectors.toList());
        if (globalTables.isEmpty()) return;

        MySQLTable outer = Randomly.fromList(globalTables);
        MySQLColumn outerCol = Randomly.fromList(outer.getColumns());
        MySQLColumn innerCol = Randomly.fromList(s1Table.getColumns());

        // 用事务保证两次查询看到相同的 outer 表快照
        state.executeStatement(new SQLQueryAdapter("START TRANSACTION", true));
        try {
            String q1 = String.format("SELECT COUNT(*) FROM %s WHERE `%s` IN (SELECT `%s` FROM %s)",
                outer.getName(), outerCol.getName(), innerCol.getName(), s1Name);
            String q2 = String.format("SELECT COUNT(*) FROM %s WHERE `%s` IN (SELECT `%s` FROM %s)",
                outer.getName(), outerCol.getName(), innerCol.getName(), s2Name);
            Long c1 = executeSingleLong(q1);
            Long c2 = executeSingleLong(q2);
            if (c1 != null && c2 != null && c1 > c2) {
                throw new AssertionError(String.format(
                    "IN subquery violation: S1 matches %d rows > S2 matches %d rows\n  Q1: %s\n  Q2: %s",
                    c1, c2, q1, q2));
            }
        } finally {
            state.executeStatement(new SQLQueryAdapter("ROLLBACK", true));
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
                    // vals.add("' '");           // 空格
                    // vals.add("'a'");
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
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);
        state.getState().logStatement(query);
        SQLancerResultSet rs = new SQLQueryAdapter(query, errors).executeAndGet(state);
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
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);
        state.getState().logStatement(query);
        SQLancerResultSet rs = new SQLQueryAdapter(query, errors).executeAndGet(state);
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
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);
        state.getState().logStatement(query);
        SQLancerResultSet rs = new SQLQueryAdapter(query, errors).executeAndGet(state);
        if (rs == null) {
            // null 意味着查询执行失败（如 ERROR 1690 算术溢出）
            // 抛出异常而不是返回空集，让 verifySelectSubset 能区分
            // "查询失败" 和 "查询成功但结果为空" 这两种情况
            throw new SQLException("Query failed (null ResultSet): " + query);
        }
        try {
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= colCount; i++) {
                    if (i > 1) row.append("|");
                    String val = rs.getString(i);
                    if (val != null) val = val.stripTrailing();
                    row.append(val);
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