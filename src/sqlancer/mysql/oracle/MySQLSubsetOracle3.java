package sqlancer.mysql.oracle;

import java.sql.SQLException;
import java.sql.Statement;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.gen.MySQLExpressionGenerator;
import sqlancer.mysql.gen.MySQLTableGenerator;

/**
 * Subset Oracle 3: Temporal subset + statistics refresh.
 *
 * <p>The oracle keeps one table and compares a baseline query before and after
 * append-only inserts plus {@code ANALYZE TABLE}. This keeps the subset idea of
 * Oracle1, while moving the subset relation into two temporal states of the
 * same table: S1 ⊆ S2.
 *
 * <p>Current core checks:
 * COUNT, MAX, MIN, and row-set containment for one fixed query shape.
 *
 * <p>Verbose output is enabled by default. Disable via system property:
 * {@code -Dsqlancer.subset.verbose=false}
 */
public class MySQLSubsetOracle3 implements TestOracle<MySQLGlobalState> {

    private static final boolean VERBOSE =
            Boolean.parseBoolean(System.getProperty("sqlancer.subset.verbose", "true"));
    private static final boolean SUPPRESS_KNOWN_FLOAT_ZERO_MAX_NULL_BUG =
            Boolean.parseBoolean(System.getProperty("sqlancer.subset3.suppressKnownFloatZeroMaxNullBug", "true"));

    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger(0);
    private static final int TARGET_BASELINE_QUERIES = 6;
    private static final int MIN_BASELINE_QUERIES = 3;
    private static final int MAX_QUERY_GENERATION_ATTEMPTS = 72;
    private static final int BASELINE_RANDOM_ROWS = 4;
    private static final int BASELINE_HOT_ROWS = 2;
    private static final int BASELINE_NOISE_ROWS = 4;
    private static final int SKEWED_EXPANSION_ROWS = 2000;
    private static final double UNCHANGED_PLAN_VERIFICATION_PROBABILITY = 0.15;

    private final MySQLGlobalState state;
    private final ExpectedErrors insertErrors;
    private String lastQueryString;

    private static final class QuerySpec {
        private final String tableName;
        private final String whereClause;
        private final String selectQuery;
        private final List<MySQLColumn> resultColumns;
        private final MySQLColumn predicateColumn;

        private QuerySpec(String tableName, String whereClause, String selectQuery, List<MySQLColumn> resultColumns,
                MySQLColumn predicateColumn) {
            this.tableName = tableName;
            this.whereClause = whereClause;
            this.selectQuery = selectQuery;
            this.resultColumns = resultColumns;
            this.predicateColumn = predicateColumn;
        }

        private String getCountQuery() {
            return "SELECT COUNT(*) FROM " + tableName + whereClause;
        }

        private String getMaxQuery(String col) {
            return "SELECT MAX(`" + col + "`) FROM " + tableName + whereClause;
        }

        private String getMinQuery(String col) {
            return "SELECT MIN(`" + col + "`) FROM " + tableName + whereClause;
        }
    }

    private static final class QuerySnapshot {
        private final Long count;
        private final Set<String> rowDigests;
        private final Map<String, Double> maxValues;
        private final Map<String, Double> minValues;
        private final List<String> plan;

        private QuerySnapshot(Long count, Set<String> rowDigests, Map<String, Double> maxValues,
                Map<String, Double> minValues, List<String> plan) {
            this.count = count;
            this.rowDigests = rowDigests;
            this.maxValues = maxValues;
            this.minValues = minValues;
            this.plan = plan;
        }
    }

    private static final class BaselinePhase {
        private final QuerySpec query;
        private final QuerySnapshot snapshot;

        private BaselinePhase(QuerySpec query, QuerySnapshot snapshot) {
            this.query = query;
            this.snapshot = snapshot;
        }
    }

    private static final class SkewProfile {
        private final MySQLColumn predicateColumn;
        private final String primaryHotValue;
        private final String secondaryHotValue;
        private final String tertiaryHotValue;
        private final PredicateShiftMode shiftMode;
        private final Map<String, List<String>> hotValuesByColumn;

        private SkewProfile(MySQLColumn predicateColumn, String primaryHotValue, String secondaryHotValue,
                String tertiaryHotValue, PredicateShiftMode shiftMode,
                Map<String, List<String>> hotValuesByColumn) {
            this.predicateColumn = predicateColumn;
            this.primaryHotValue = primaryHotValue;
            this.secondaryHotValue = secondaryHotValue;
            this.tertiaryHotValue = tertiaryHotValue;
            this.shiftMode = shiftMode;
            this.hotValuesByColumn = hotValuesByColumn;
        }
    }

    private enum DistributionStage {
        BASELINE,
        EXPANSION
    }

    private enum PredicateShiftMode {
        PRIMARY_HEAVY,
        PRIMARY_SECONDARY_SPLIT,
        SECONDARY_HEAVY,
        NULL_HEAVY,
        NOISE_HEAVY
    }

    private enum ProjectionStyle {
        ALL_COLUMNS,
        PREDICATE_ONLY,
        PREDICATE_PLUS_ONE,
        RANDOM_SUBSET,
        NON_PREDICATE_FOCUS
    }

    public MySQLSubsetOracle3(MySQLGlobalState state) {
        this.state = state;
        this.insertErrors = new ExpectedErrors();
        MySQLErrors.addInsertUpdateErrors(insertErrors);
        MySQLErrors.addExpressionErrors(insertErrors);
        insertErrors.add("Incorrect string value");
        insertErrors.add("Incorrect double value");
        insertErrors.add("Incorrect integer value");
        insertErrors.add("Incorrect decimal value");
        insertErrors.add("Invalid utf8");
        insertErrors.add("Cannot convert");
        insertErrors.add("is not valid for CHARACTER SET");
        insertErrors.add("doesn't have this option");
        insertErrors.add("DELAYED option");
    }

    @Override
    public void check() throws Exception {
        int id = TABLE_COUNTER.incrementAndGet();
        String tableName = "subset3_" + id;

        log("================================================================");
        log(" SUBSET ORACLE 3 round #" + id);
        log("================================================================");

        try {
            log("\n[Step 1] Creating temporal table: " + tableName);
            SQLQueryAdapter createTable;
            do {
                createTable = MySQLTableGenerator.generate(state, tableName);
            } while (createTable.getQueryString().toUpperCase().contains(" LIKE "));
            logSQL(createTable.getQueryString());
            state.executeStatement(createTable);

            MySQLTable table = findTable(tableName);
            if (table == null) {
                throw new IgnoreMeException();
            }

            List<MySQLColumn> numericCols = table.getColumns().stream()
                    .filter(c -> c.getType().isNumeric())
                    .collect(Collectors.toList());
            log("  Created with columns: " + table.getColumns().stream()
                    .map(MySQLColumn::getName).collect(Collectors.toList()));

            MySQLColumn predicateColumn = choosePredicateColumn(table);
            ensureSupportingIndexes(table, predicateColumn, id);
            SkewProfile skewProfile = createSkewProfile(table, predicateColumn);
            log("  Biased predicate column: " + predicateColumn.getName()
                    + " with hot value " + skewProfile.primaryHotValue);

            log("\n[Step 2] Building baseline state S1 with append-only inserts...");
            appendHotSeedRows(table, skewProfile, BASELINE_HOT_ROWS);
            appendSkewedRows(table, skewProfile, BASELINE_RANDOM_ROWS + Randomly.smallNumber(), 0.35,
                    DistributionStage.BASELINE);
            log("  Inserting a few boundary/null seed rows into S1...");
            insertNoiseRows(table, BASELINE_NOISE_ROWS);
            Long s1TableCount = executeSingleLong("SELECT COUNT(*) FROM " + tableName);
            if (s1TableCount == null || s1TableCount == 0L) {
                throw new IgnoreMeException();
            }
            log("  S1 row count: " + s1TableCount);

            log("\n[Step 3] Selecting baseline query Q and collecting Res1 / Plan1...");
            List<BaselinePhase> baselines = createValidatedBaselinePhases(table, numericCols, skewProfile);
            log("  Validated baseline queries: " + baselines.size());
            for (int i = 0; i < baselines.size(); i++) {
                BaselinePhase baseline = baselines.get(i);
                log("  Baseline[" + (i + 1) + "] count: " + baseline.snapshot.count);
                logPlan("Plan1[" + (i + 1) + "]", baseline.snapshot.plan);
            }

            log("\n[Step 4] Appending many skewed rows, COMMIT, and ANALYZE TABLE...");
            state.executeStatement(new SQLQueryAdapter("START TRANSACTION", false));
            appendSkewedRows(table, skewProfile, SKEWED_EXPANSION_ROWS + 64 * Randomly.smallNumber(), 0.92,
                    DistributionStage.EXPANSION);
            state.executeStatement(new SQLQueryAdapter("COMMIT", false));

            Long s2TableCount = executeSingleLong("SELECT COUNT(*) FROM " + tableName);
            if (s2TableCount == null || s2TableCount <= s1TableCount) {
                log("  Table did not grow enough to form a useful S1 -> S2 transition; skipping round");
                throw new IgnoreMeException();
            }
            log("  S2 row count before ANALYZE: " + s2TableCount);
            log("  Growth ratio S2/S1: " + String.format("%.2f", (double) s2TableCount / s1TableCount));

            analyzeTable(tableName);

            log("\n[Step 5] Re-running the same queries on S2...");
            for (int i = 0; i < baselines.size(); i++) {
                BaselinePhase baseline = baselines.get(i);
                log("  Query[" + (i + 1) + "]: " + baseline.query.selectQuery);
                List<String> s2Plan = captureExplainPlan(baseline.query.selectQuery);
                logPlan("Plan2[" + (i + 1) + "]", s2Plan);
                boolean planChanged = !plansEquivalentStructurally(baseline.snapshot.plan, s2Plan);
                log("  Plan changed after ANALYZE TABLE [" + (i + 1) + "]: " + planChanged);
                logPlanComparison(i + 1, baseline.query.selectQuery, baseline.snapshot.plan, s2Plan, planChanged);
                if (!planChanged) {
                    boolean sampled = Randomly.getPercentage() < UNCHANGED_PLAN_VERIFICATION_PROBABILITY;
                    log("  Plan unchanged sampling decision [" + (i + 1) + "]: "
                            + (sampled ? "verify" : "skip"));
                    if (!sampled) {
                        continue;
                    }
                }
                QuerySnapshot s2Snapshot = executeSnapshot("S2", baseline.query, numericCols, false, s2Plan);
                log("  S2 count[" + (i + 1) + "]: " + s2Snapshot.count);
                suppressKnownReportedBug(table, baseline.query, baseline.snapshot, s2Snapshot);

                log("\n[Step 6] Checking monotonicity across S1 ⊆ S2...");
                log("\n[Step 6." + (i + 1) + "] Checking monotonicity across S1/S2...");
                verifyCount(baseline.query, baseline.snapshot, s2Snapshot);
                for (MySQLColumn col : numericCols) {
                    verifyMax(baseline.query, col.getName(), baseline.snapshot, s2Snapshot);
                    verifyMin(baseline.query, col.getName(), baseline.snapshot, s2Snapshot);
                }
                verifySelectSubset(baseline.query, baseline.snapshot, s2Snapshot);
            }

            // TODO: Extend Oracle3 with COUNT DISTINCT, IN-subquery, and join-based
            // checks once the core temporal-state workflow stabilizes.
            log("\n  All Oracle3 core checks PASSED for round #" + id);

        } catch (java.sql.SQLNonTransientConnectionException e) {
            throw new IgnoreMeException();
        } catch (com.mysql.cj.jdbc.exceptions.CommunicationsException e) {
            throw new IgnoreMeException();
        } catch (java.sql.SQLRecoverableException e) {
            throw new IgnoreMeException();
        } catch (java.sql.SQLException e) {
            if (e.getMessage() != null && e.getMessage().contains("NumberFormatException")) {
                throw new IgnoreMeException();
            }
            throw e;
        } finally {
            log("\n[Cleanup] Dropping temporary table " + tableName + "...");
            dropIfExists(tableName);
            log("================================================================\n");
        }
    }

    private List<BaselinePhase> createValidatedBaselinePhases(MySQLTable table, List<MySQLColumn> numericCols,
            SkewProfile skewProfile) throws Exception {
        Map<String, BaselinePhase> phases = new LinkedHashMap<>();
        for (int attempt = 0; attempt < MAX_QUERY_GENERATION_ATTEMPTS && phases.size() < TARGET_BASELINE_QUERIES;
                attempt++) {
            QuerySpec candidate = buildQuerySpec(table, skewProfile);
            if (phases.containsKey(candidate.selectQuery)) {
                continue;
            }
            try {
                QuerySnapshot snapshot = executeSnapshot("S1", candidate, numericCols, true);
                if (snapshot.count == null || snapshot.count == 0L) {
                    log("  Query candidate #" + (attempt + 1) + " skipped: empty baseline result");
                    continue;
                }
                phases.put(candidate.selectQuery, new BaselinePhase(candidate, snapshot));
            } catch (Throwable t) {
                log("  Query candidate #" + (attempt + 1) + " skipped: "
                        + t.getClass().getSimpleName() + ": " + t.getMessage());
            }
        }
        if (phases.size() < MIN_BASELINE_QUERIES) {
            throw new IgnoreMeException();
        }
        return new ArrayList<>(phases.values());
    }

    private QuerySpec buildQuerySpec(MySQLTable table, SkewProfile skewProfile) {
        String whereClause = buildWhereClause(skewProfile);
        List<MySQLColumn> projectionColumns = chooseProjectionColumns(table, skewProfile.predicateColumn);
        String selectColumns = projectionColumns.stream()
                .map(c -> "`" + c.getName() + "`")
                .collect(Collectors.joining(", "));
        String selectQuery = "SELECT " + selectColumns + " FROM " + table.getName() + whereClause;
        logSQL(selectQuery);
        return new QuerySpec(table.getName(), whereClause, selectQuery, projectionColumns, skewProfile.predicateColumn);
    }

    private String buildWhereClause(SkewProfile skewProfile) {
        MySQLColumn predicate = skewProfile.predicateColumn;
        switch (predicate.getType()) {
        case INT:
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
            return buildNumericWhereClause(predicate, skewProfile);
        case VARCHAR:
            return buildVarcharWhereClause(predicate, skewProfile);
        default:
            return " WHERE `" + predicate.getName() + "` = " + skewProfile.primaryHotValue;
        }
    }

    private String buildNumericWhereClause(MySQLColumn predicate, SkewProfile skewProfile) {
        String col = "`" + predicate.getName() + "`";
        switch (Randomly.fromOptions(0, 1, 2, 3, 4, 5)) {
        case 0:
            return " WHERE " + col + " = " + skewProfile.primaryHotValue;
        case 1:
            return " WHERE " + col + " IN (" + skewProfile.primaryHotValue + ", " + skewProfile.secondaryHotValue
                    + ")";
        case 2:
            return " WHERE " + col + " BETWEEN " + skewProfile.primaryHotValue + " AND "
                    + skewProfile.secondaryHotValue;
        case 3:
            return " WHERE " + col + " <= " + skewProfile.secondaryHotValue;
        case 4:
            return " WHERE " + col + " >= " + skewProfile.primaryHotValue;
        default:
            return " WHERE " + col + " IS NULL";
        }
    }

    private String buildVarcharWhereClause(MySQLColumn predicate, SkewProfile skewProfile) {
        String col = "`" + predicate.getName() + "`";
        String primary = skewProfile.primaryHotValue;
        String secondary = skewProfile.secondaryHotValue;
        String primaryLiteral = unquoteLiteral(primary);
        String prefix = primaryLiteral.isEmpty() ? "h" : primaryLiteral.substring(0, 1);
        switch (Randomly.fromOptions(0, 1, 2, 3, 4)) {
        case 0:
            return " WHERE " + col + " = " + primary;
        case 1:
            return " WHERE " + col + " IN (" + primary + ", " + secondary + ")";
        case 2:
            return " WHERE " + col + " LIKE '" + prefix + "%'";
        case 3:
            return " WHERE " + col + " >= " + primary;
        default:
            return " WHERE " + col + " IS NULL";
        }
    }

    private List<MySQLColumn> chooseProjectionColumns(MySQLTable table, MySQLColumn predicateColumn) {
        List<MySQLColumn> allColumns = table.getColumns();
        ProjectionStyle style = Randomly.fromOptions(ProjectionStyle.values());
        LinkedHashSet<MySQLColumn> chosen = new LinkedHashSet<>();
        switch (style) {
        case ALL_COLUMNS:
            chosen.addAll(allColumns);
            break;
        case PREDICATE_ONLY:
            chosen.add(predicateColumn);
            break;
        case PREDICATE_PLUS_ONE:
            chosen.add(predicateColumn);
            allColumns.stream().filter(c -> !c.equals(predicateColumn)).findAny().ifPresent(chosen::add);
            break;
        case RANDOM_SUBSET:
            chosen.add(predicateColumn);
            List<MySQLColumn> shuffled = Randomly.nonEmptySubset(allColumns);
            chosen.addAll(shuffled.stream().limit(1 + Randomly.smallNumber()).collect(Collectors.toList()));
            break;
        case NON_PREDICATE_FOCUS:
            List<MySQLColumn> nonPredicate = allColumns.stream()
                    .filter(c -> !c.equals(predicateColumn))
                    .collect(Collectors.toList());
            if (nonPredicate.isEmpty()) {
                chosen.add(predicateColumn);
            } else {
                chosen.addAll(Randomly.nonEmptySubset(nonPredicate).stream()
                        .limit(Math.min(nonPredicate.size(), 1 + Randomly.smallNumber()))
                        .collect(Collectors.toList()));
            }
            break;
        default:
            chosen.addAll(allColumns);
            break;
        }
        if (chosen.isEmpty()) {
            chosen.add(predicateColumn);
        }
        return new ArrayList<>(chosen);
    }

    private String unquoteLiteral(String literal) {
        if (literal == null || literal.length() < 2) {
            return literal == null ? "" : literal;
        }
        if ((literal.startsWith("'") && literal.endsWith("'"))
                || (literal.startsWith("\"") && literal.endsWith("\""))) {
            return literal.substring(1, literal.length() - 1);
        }
        return literal;
    }

    private QuerySnapshot executeSnapshot(String label, QuerySpec query, List<MySQLColumn> numericCols,
            boolean captureRows) throws Exception {
        return executeSnapshot(label, query, numericCols, captureRows, null);
    }

    private QuerySnapshot executeSnapshot(String label, QuerySpec query, List<MySQLColumn> numericCols,
            boolean captureRows, List<String> precomputedPlan) throws Exception {
        Long count = executeSingleLong(query.getCountQuery());
        Set<String> rowDigests = captureRows ? executeAndGetRowDigests(query.selectQuery, query.resultColumns) : null;
        Map<String, Double> maxValues = new LinkedHashMap<>();
        Map<String, Double> minValues = new LinkedHashMap<>();
        for (MySQLColumn col : numericCols) {
            maxValues.put(col.getName(), executeSingleDouble(query.getMaxQuery(col.getName())));
            minValues.put(col.getName(), executeSingleDouble(query.getMinQuery(col.getName())));
        }
        List<String> plan = precomputedPlan != null ? precomputedPlan : captureExplainPlan(query.selectQuery);
        if (rowDigests != null) {
            log("  " + label + " result rows: " + rowDigests.size());
        }
        return new QuerySnapshot(count, rowDigests, maxValues, minValues, plan);
    }

    private void suppressKnownReportedBug(MySQLTable table, QuerySpec query, QuerySnapshot s1, QuerySnapshot s2)
            throws Exception {
        if (!SUPPRESS_KNOWN_FLOAT_ZERO_MAX_NULL_BUG) {
            return;
        }
        String floatingPredicateDescription = extractFloatingPredicateDescription(query);
        if (floatingPredicateDescription == null) {
            return;
        }
        String affectedColumn = findKnownFloatMaxNullBugColumn(table, query, s1, s2);
        if (affectedColumn == null) {
            return;
        }
        log("  Known reported MySQL bug pattern detected (FLOAT/DOUBLE range/equality index path leaks NULL MAX).");
        log("  Floating predicate: " + floatingPredicateDescription + ", affected column: " + affectedColumn);
        log("  Suppressing this round so Oracle3 can continue searching for new bugs.");
        throw new IgnoreMeException();
    }

    private String extractFloatingPredicateDescription(QuerySpec query) {
        if (query.predicateColumn == null || !isFloatingPointColumn(query.predicateColumn)) {
            return null;
        }
        String prefix = " WHERE `" + query.predicateColumn.getName() + "` ";
        if (!query.whereClause.startsWith(prefix)) {
            return null;
        }
        String predicate = query.whereClause.substring(prefix.length()).trim();
        if (predicate.isEmpty() || "IS NULL".equalsIgnoreCase(predicate)) {
            return null;
        }
        if (predicate.startsWith("=") || predicate.startsWith("IN (") || predicate.startsWith("BETWEEN ")
                || predicate.startsWith("<=") || predicate.startsWith(">=")) {
            return predicate;
        }
        return null;
    }

    private String findKnownFloatMaxNullBugColumn(MySQLTable table, QuerySpec query, QuerySnapshot s1, QuerySnapshot s2)
            throws SQLException {
        if (s1.count == null || s1.count <= 0 || s2.count == null || s2.count <= 0) {
            return null;
        }
        MySQLTable currentTable = findTable(query.tableName);
        MySQLTable tableWithFreshIndexes = currentTable != null ? currentTable : table;
        for (MySQLColumn col : tableWithFreshIndexes.getColumns()) {
            if (!isFloatingPointColumn(col)) {
                continue;
            }
            Double s1Max = s1.maxValues.get(col.getName());
            Double s2Max = s2.maxValues.get(col.getName());
            if (s1Max == null || s2Max != null) {
                continue;
            }
            Double ignoreIndexMax = executeSingleDouble(buildIgnoreIndexAggregateQuery(tableWithFreshIndexes, query,
                    "MAX", col.getName()));
            if (ignoreIndexMax != null && ignoreIndexMax + 1e-9 >= s1Max) {
                return col.getName();
            }
        }
        return null;
    }

    private String buildIgnoreIndexAggregateQuery(MySQLTable table, QuerySpec query, String aggregate, String column) {
        String ignoreIndexClause = fetchCurrentIndexNames(query.tableName).stream()
                .collect(Collectors.joining(", "));
        if (ignoreIndexClause.isEmpty()) {
            return "SELECT " + aggregate + "(`" + column + "`) FROM " + query.tableName + query.whereClause;
        }
        return "SELECT " + aggregate + "(`" + column + "`) FROM " + query.tableName
                + " IGNORE INDEX (" + ignoreIndexClause + ")" + query.whereClause;
    }

    private List<String> fetchCurrentIndexNames(String tableName) {
        List<String> indexes = new ArrayList<>();
        String sql = "SHOW INDEX FROM " + tableName;
        try (Statement s = state.getConnection().createStatement()) {
            int timeoutSeconds = Integer.getInteger("sqlancer.jdbc.queryTimeoutSeconds", 600);
            if (timeoutSeconds > 0) {
                s.setQueryTimeout(timeoutSeconds);
            }
            try (java.sql.ResultSet rs = s.executeQuery(sql)) {
                while (rs.next()) {
                    String indexName = rs.getString("Key_name");
                    if (indexName != null && !indexes.contains(indexName)) {
                        indexes.add(indexName);
                    }
                }
            }
        } catch (SQLException e) {
            log("  Failed to fetch current indexes for suppression: " + e.getMessage());
        }
        return indexes;
    }

    private void verifyCount(QuerySpec query, QuerySnapshot s1, QuerySnapshot s2) {
        if (s1.count == null || s2.count == null) {
            log("  COUNT(*) skipped because one result was null");
            return;
        }
        boolean pass = s1.count <= s2.count;
        log(String.format("  COUNT(*)   S1=%-6s  <= S2=%-6s   [%s]",
                s1.count, s2.count, pass ? "PASS" : "FAIL"));
        if (!pass) {
            lastQueryString = query.selectQuery;
            throw new AssertionError(String.format(
                    "Oracle3 COUNT violation: COUNT(S1)=%d > COUNT(S2)=%d%n"
                    + "  Query: %s%n  Plan1: %s%n  Plan2: %s",
                    s1.count, s2.count, query.selectQuery, formatPlan(s1.plan), formatPlan(s2.plan)));
        }
    }

    private void verifyMax(QuerySpec query, String col, QuerySnapshot s1, QuerySnapshot s2) {
        Double max1 = s1.maxValues.get(col);
        Double max2 = s2.maxValues.get(col);
        if (max1 == null) {
            log(String.format("  MAX(%-6s) skipped because S1 is NULL", col));
            return;
        }
        boolean pass = max2 != null && max1 <= max2 + 1e-9;
        log(String.format("  MAX(%-6s) S1=%-12s  <= S2=%-12s   [%s]",
                col, max1, max2, pass ? "PASS" : "FAIL"));
        if (!pass) {
            lastQueryString = query.selectQuery;
            throw new AssertionError(String.format(
                    "Oracle3 MAX violation on %s: S1=%s > S2=%s%n"
                    + "  Query: %s%n  Plan1: %s%n  Plan2: %s",
                    col, max1, max2, query.selectQuery, formatPlan(s1.plan), formatPlan(s2.plan)));
        }
    }

    private void verifyMin(QuerySpec query, String col, QuerySnapshot s1, QuerySnapshot s2) {
        Double min1 = s1.minValues.get(col);
        Double min2 = s2.minValues.get(col);
        if (min1 == null) {
            log(String.format("  MIN(%-6s) skipped because S1 is NULL", col));
            return;
        }
        boolean pass = min2 != null && min2 <= min1 + 1e-9;
        log(String.format("  MIN(%-6s) S1=%-12s  >= S2=%-12s   [%s]",
                col, min1, min2, pass ? "PASS" : "FAIL"));
        if (!pass) {
            lastQueryString = query.selectQuery;
            throw new AssertionError(String.format(
                    "Oracle3 MIN violation on %s: S1=%s < S2=%s%n"
                    + "  Query: %s%n  Plan1: %s%n  Plan2: %s",
                    col, min1, min2, query.selectQuery, formatPlan(s1.plan), formatPlan(s2.plan)));
        }
    }

    private void verifySelectSubset(QuerySpec query, QuerySnapshot s1, QuerySnapshot s2) throws SQLException {
        Set<String> missing = removePresentRowDigests(query.selectQuery, query.resultColumns, s1.rowDigests);
        boolean pass = missing.isEmpty();
        log(String.format("  ROW-SET    |S1|=%-6d subset |S2|=%-6d   [%s]",
                s1.rowDigests.size(), s2.count, pass ? "PASS" : "FAIL"));
        if (!pass) {
            lastQueryString = query.selectQuery;
            throw new AssertionError(String.format(
                    "Oracle3 row-set subset violation: Res1 is not a subset of Res2%n"
                    + "  Missing row digests: %s%n  Query: %s%n  Plan1: %s%n  Plan2: %s",
                    missing, query.selectQuery, formatPlan(s1.plan), formatPlan(s2.plan)));
        }
    }

    private void appendHotSeedRows(MySQLTable table, SkewProfile skewProfile, int nrRows) {
        for (int i = 0; i < nrRows; i++) {
            try {
                MySQLExpressionGenerator gen = new MySQLExpressionGenerator(state).setColumns(table.getColumns());
                List<String> values = new ArrayList<>();
                for (MySQLColumn col : table.getColumns()) {
                    if (col.getName().equals(skewProfile.predicateColumn.getName())) {
                        values.add(skewProfile.primaryHotValue);
                    } else {
                        values.add(generateValue(col, gen, skewProfile, 0.5, DistributionStage.BASELINE));
                    }
                }
                executeInsert(table, values);
            } catch (Throwable e) {
                log("  Hot seed INSERT skipped: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
    }

    private void appendSkewedRows(MySQLTable table, SkewProfile skewProfile, int nrRows, double hotspotProbability,
            DistributionStage stage) {
        List<MySQLColumn> cols = table.getColumns();
        for (int i = 0; i < nrRows; i++) {
            try {
                MySQLExpressionGenerator gen = new MySQLExpressionGenerator(state).setColumns(cols);
                List<String> values = cols.stream()
                        .map(c -> generateValue(c, gen, skewProfile, hotspotProbability, stage))
                        .collect(Collectors.toList());
                executeInsert(table, values);
            } catch (Throwable e) {
                log("  Skewed INSERT skipped: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
    }

    private void analyzeTable(String tableName) throws Exception {
        String analyzeSql = "ANALYZE TABLE " + tableName;
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);
        logSQL(analyzeSql);
        state.executeStatement(new SQLQueryAdapter(analyzeSql, errors));
    }

    private List<String> captureExplainPlan(String selectQuery) throws SQLException {
        String explainQuery = "EXPLAIN FORMAT=TRADITIONAL " + selectQuery;
        lastQueryString = explainQuery;
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);
        state.getState().logStatement(explainQuery);
        SQLancerResultSet rs = new SQLQueryAdapter(explainQuery, errors).executeAndGet(state);
        if (rs == null) {
            throw new SQLException("EXPLAIN returned null: " + explainQuery);
        }
        try {
            List<String> planRows = new ArrayList<>();
            while (rs.next()) {
                // 列号修正：4=partitions(跳过), 5=type, 7=key
                String row = String.format(
                    "id=%s;select_type=%s;table=%s;type=%s;possible_keys=%s;key=%s;key_len=%s;rows=%s;filtered=%s;extra=%s",
                    nullToEmpty(getOptionalString(rs, 1)),   // id
                    nullToEmpty(getOptionalString(rs, 2)),   // select_type
                    nullToEmpty(getOptionalString(rs, 3)),   // table
                    nullToEmpty(getOptionalString(rs, 5)),   // type (ALL/index/range/ref...) ← 原来是4，错的
                    nullToEmpty(getOptionalString(rs, 6)),   // possible_keys
                    nullToEmpty(getOptionalString(rs, 7)),   // key（实际使用的索引）← 原来是6，错的
                    nullToEmpty(getOptionalString(rs, 8)),   // key_len
                    nullToEmpty(getOptionalString(rs, 10)),  // rows
                    nullToEmpty(getOptionalString(rs, 11)),  // filtered
                    nullToEmpty(getOptionalString(rs, 12))   // Extra
                );
                planRows.add(row);
            }
            return planRows;
        } finally {
            rs.close();
        }
    }

    private void logPlanComparison(int index, String query,
            List<String> plan1, List<String> plan2, boolean changed) {
        log(String.format("\n  [Plan Diff #%d] planChanged=%s", index, changed));
        log("  Query: " + query);
        int maxRows = Math.max(plan1.size(), plan2.size());
        for (int r = 0; r < maxRows; r++) {
            String p1row = r < plan1.size() ? plan1.get(r) : "(none)";
            String p2row = r < plan2.size() ? plan2.get(r) : "(none)";
            boolean rowDiff = !normalizePlanRow(p1row).equals(normalizePlanRow(p2row));
            if (rowDiff) {
                log(String.format("    row[%d] <<< CHANGED >>>", r));
                log("      S1: " + p1row);
                log("      S2: " + p2row);
            } else {
                log(String.format("    row[%d] same: %s", r, p1row));
            }
        }
    }

    private boolean plansEquivalentStructurally(List<String> left, List<String> right) {
        if (left == null || right == null) {
            return left == right;
        }
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (!normalizePlanRow(left.get(i)).equals(normalizePlanRow(right.get(i)))) {
                return false;
            }
        }
        return true;
    }

    private String normalizePlanRow(String row) {
        // rows/filtered 随数据量变化，不代表路径切换，抹掉
        // type 和 key 是路径切换的核心标志，保留
        return row
            .replaceAll("rows=[^;]+", "rows=?")
            .replaceAll("filtered=[^;]+", "filtered=?")
            .replaceAll("key_len=[^;]+", "key_len=?")
            .replaceAll("\\s+", " ")
            .trim();
    }

    private void insertNoiseRows(MySQLTable table, int nrRows) {
        List<MySQLColumn> cols = table.getColumns();
        Map<MySQLColumn, List<String>> boundaryMap = new LinkedHashMap<>();
        for (MySQLColumn col : cols) {
            List<String> vals = new ArrayList<>();
            switch (col.getType()) {
            case INT:
                vals.add("0");
                vals.add("1");
                vals.add("-1");
                vals.add("-2147483648");
                vals.add("2147483647");
                vals.add("-9223372036854775808");
                vals.add("9223372036854775807");
                vals.add("NULL");
                break;
            case VARCHAR:
                vals.add("''");
                vals.add("'" + "a".repeat(500) + "'");
                vals.add("'%'");
                vals.add("'_'");
                vals.add("'NULL'");
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
                vals.add("3.4028235E38");
                vals.add("-3.4028235E38");
                vals.add("1.7976931348623157E308");
                vals.add("1.4E-45");
                vals.add("NULL");
                break;
            case DECIMAL:
                vals.add("0");
                vals.add("0.00");
                vals.add("1.00");
                vals.add("-1.00");
                vals.add("99999999999999999999999999999999999.99");
                vals.add("-99999999999999999999999999999999999.99");
                vals.add("NULL");
                break;
            default:
                vals.add("NULL");
                break;
            }
            boundaryMap.put(col, vals);
        }

        for (int i = 0; i < nrRows; i++) {
            MySQLColumn targetCol = Randomly.fromList(cols);
            String boundaryVal = Randomly.fromList(boundaryMap.get(targetCol));
            List<String> values = new ArrayList<>();
            for (MySQLColumn col : cols) {
                values.add(col.equals(targetCol) ? boundaryVal : "NULL");
            }
            try {
                executeInsert(table, values);
            } catch (Throwable e) {
                log("  Noise INSERT skipped: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
    }

    private SkewProfile createSkewProfile(MySQLTable table, MySQLColumn predicateColumn) {
        Map<String, List<String>> hotValuesByColumn = new LinkedHashMap<>();
        for (MySQLColumn col : table.getColumns()) {
            hotValuesByColumn.put(col.getName(), createHotValues(col));
        }
        List<String> predicateHotValues = hotValuesByColumn.get(predicateColumn.getName());
        String primaryHotValue = predicateHotValues.get(0);
        String secondaryHotValue = predicateHotValues.size() > 1 ? predicateHotValues.get(1) : primaryHotValue;
        String tertiaryHotValue = predicateHotValues.size() > 2 ? predicateHotValues.get(2) : secondaryHotValue;
        PredicateShiftMode shiftMode = Randomly.fromOptions(PredicateShiftMode.values());
        return new SkewProfile(predicateColumn, primaryHotValue, secondaryHotValue, tertiaryHotValue, shiftMode,
                hotValuesByColumn);
    }

    private List<String> createHotValues(MySQLColumn col) {
        List<String> hotValues = new ArrayList<>();
        switch (col.getType()) {
        case INT:
            int intAnchor = state.getRandomly().getInteger(-16, 17);
            hotValues.add(String.valueOf(intAnchor));
            hotValues.add(String.valueOf(intAnchor + 1 + Randomly.smallNumber()));
            hotValues.add(String.valueOf(intAnchor - 1 - Randomly.smallNumber()));
            break;
        case VARCHAR:
            String stringStem = "hv_" + Math.abs(state.getRandomly().getInteger());
            hotValues.add(quoteStringLiteral(stringStem));
            hotValues.add(quoteStringLiteral(stringStem + "_a"));
            hotValues.add(quoteStringLiteral(stringStem + "_b"));
            break;
        case FLOAT:
        case DOUBLE:
            double floatAnchor = state.getRandomly().getInteger(-200, 201) / 10.0;
            hotValues.add(formatFloatingHotValue(floatAnchor));
            hotValues.add(formatFloatingHotValue(floatAnchor + 1.0 + Randomly.smallNumber()));
            hotValues.add(formatFloatingHotValue(floatAnchor - 1.0 - Randomly.smallNumber()));
            break;
        case DECIMAL:
            double decimalAnchor = state.getRandomly().getInteger(-1000, 1001) / 100.0;
            hotValues.add(formatDecimalHotValue(decimalAnchor));
            hotValues.add(formatDecimalHotValue(decimalAnchor + 1.0 + Randomly.smallNumber()));
            hotValues.add(formatDecimalHotValue(decimalAnchor - 1.0 - Randomly.smallNumber()));
            break;
        default:
            hotValues.add("NULL");
            break;
        }
        return hotValues;
    }

    private String formatFloatingHotValue(double value) {
        return String.format(Locale.US, "%.3f", value);
    }

    private String formatDecimalHotValue(double value) {
        return String.format(Locale.US, "%.2f", value);
    }

    private String quoteStringLiteral(String value) {
        String escaped = value.replace("\\", "\\\\").replace("'", "''");
        return "'" + escaped + "'";
    }

    private MySQLColumn choosePredicateColumn(MySQLTable table) {
        List<MySQLColumn> preferredCols = table.getColumns().stream()
                .filter(c -> !c.isPrimaryKey())
                .filter(c -> c.getType().isNumeric() || c.getType() == sqlancer.mysql.MySQLSchema.MySQLDataType.VARCHAR)
                .collect(Collectors.toList());
        if (!preferredCols.isEmpty()) {
            return Randomly.fromList(preferredCols);
        }
        List<MySQLColumn> nonPrimaryCols = table.getColumns().stream()
                .filter(c -> !c.isPrimaryKey())
                .collect(Collectors.toList());
        if (!nonPrimaryCols.isEmpty()) {
            return Randomly.fromList(nonPrimaryCols);
        }
        return Randomly.fromList(table.getColumns());
    }

    private void ensureSupportingIndexes(MySQLTable table, MySQLColumn predicateColumn, int id) {
        String indexName = "i_subset3_" + id;
        String createIndexSql = "CREATE INDEX " + indexName + " ON " + table.getName()
                + " (`" + predicateColumn.getName() + "`)";
        logSQL(createIndexSql);
        try {
            executeInternalDDL(createIndexSql);
        } catch (Throwable e) {
            log("  Supporting index creation skipped: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }

        List<MySQLColumn> secondaryCandidates = table.getColumns().stream()
                .filter(c -> !c.equals(predicateColumn))
                .collect(Collectors.toList());
        if (secondaryCandidates.isEmpty() || Randomly.getBooleanWithSmallProbability()) {
            return;
        }
        MySQLColumn secondaryColumn = Randomly.fromList(secondaryCandidates);
        String compositeIndexName = "i_subset3_c_" + id;
        String compositeIndexSql;
        if (Randomly.getBoolean()) {
            compositeIndexSql = "CREATE INDEX " + compositeIndexName + " ON " + table.getName()
                    + " (`" + predicateColumn.getName() + "`, `" + secondaryColumn.getName() + "`)";
        } else {
            compositeIndexSql = "CREATE INDEX " + compositeIndexName + " ON " + table.getName()
                    + " (`" + secondaryColumn.getName() + "`, `" + predicateColumn.getName() + "`)";
        }
        logSQL(compositeIndexSql);
        try {
            executeInternalDDL(compositeIndexSql);
        } catch (Throwable e) {
            log("  Composite supporting index creation skipped: " + e.getClass().getSimpleName()
                    + ": " + e.getMessage());
        }
    }

    private String generateValue(MySQLColumn col, MySQLExpressionGenerator gen, SkewProfile skewProfile,
            double hotspotProbability, DistributionStage stage) {
        if (col.getName().equals(skewProfile.predicateColumn.getName())) {
            return generatePredicateValue(gen, skewProfile, stage);
        }
        List<String> hotValues = skewProfile.hotValuesByColumn.get(col.getName());
        if (hotValues != null && Randomly.getPercentage() < hotspotProbability) {
            return Randomly.fromList(hotValues);
        }
        return MySQLVisitor.asString(gen.generateConstant());
    }

    private String generatePredicateValue(MySQLExpressionGenerator gen, SkewProfile skewProfile, DistributionStage stage) {
        double p = Randomly.getPercentage();
        if (stage == DistributionStage.BASELINE) {
            if (p < 0.80) {
                return skewProfile.primaryHotValue;
            } else if (p < 0.92) {
                return skewProfile.secondaryHotValue;
            } else if (p < 0.97) {
                return skewProfile.tertiaryHotValue;
            }
            return MySQLVisitor.asString(gen.generateConstant());
        }
        switch (skewProfile.shiftMode) {
        case PRIMARY_HEAVY:
            if (p < 0.55) {
                return skewProfile.primaryHotValue;
            } else if (p < 0.80) {
                return skewProfile.secondaryHotValue;
            } else if (p < 0.92) {
                return skewProfile.tertiaryHotValue;
            } else if (p < 0.97) {
                return "NULL";
            }
            return MySQLVisitor.asString(gen.generateConstant());
        case PRIMARY_SECONDARY_SPLIT:
            if (p < 0.35) {
                return skewProfile.primaryHotValue;
            } else if (p < 0.70) {
                return skewProfile.secondaryHotValue;
            } else if (p < 0.88) {
                return skewProfile.tertiaryHotValue;
            } else if (p < 0.95) {
                return "NULL";
            }
            return MySQLVisitor.asString(gen.generateConstant());
        case SECONDARY_HEAVY:
            if (p < 0.22) {
                return skewProfile.primaryHotValue;
            } else if (p < 0.62) {
                return skewProfile.secondaryHotValue;
            } else if (p < 0.84) {
                return skewProfile.tertiaryHotValue;
            } else if (p < 0.92) {
                return "NULL";
            }
            return MySQLVisitor.asString(gen.generateConstant());
        case NULL_HEAVY:
            if (p < 0.28) {
                return skewProfile.primaryHotValue;
            } else if (p < 0.48) {
                return skewProfile.secondaryHotValue;
            } else if (p < 0.62) {
                return skewProfile.tertiaryHotValue;
            } else if (p < 0.84) {
                return "NULL";
            }
            return MySQLVisitor.asString(gen.generateConstant());
        case NOISE_HEAVY:
        default:
            if (p < 0.22) {
                return skewProfile.primaryHotValue;
            } else if (p < 0.36) {
                return skewProfile.secondaryHotValue;
            } else if (p < 0.46) {
                return skewProfile.tertiaryHotValue;
            } else if (p < 0.54) {
                return "NULL";
            }
            return MySQLVisitor.asString(gen.generateConstant());
        }
    }

    private void executeInsert(MySQLTable table, List<String> values) throws Exception {
        StringBuilder sb = new StringBuilder("INSERT IGNORE INTO ");
        sb.append(table.getName()).append(" (");
        sb.append(table.getColumns().stream().map(c -> "`" + c.getName() + "`").collect(Collectors.joining(", ")));
        sb.append(") VALUES (");
        sb.append(values.stream().collect(Collectors.joining(", ")));
        sb.append(")");
        logSQL(sb.toString());
        state.executeStatement(new SQLQueryAdapter(sb.toString(), insertErrors));
    }

    private MySQLTable findTable(String name) {
        return state.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getName().equalsIgnoreCase(name))
                .findFirst().orElse(null);
    }

    private void dropIfExists(String tableName) {
        try {
            executeInternalDDL("DROP TABLE IF EXISTS " + tableName);
        } catch (Exception ignored) {
        }
    }

    private void executeInternalDDL(String sql) throws SQLException {
        state.getState().logStatement(sql);
        try (Statement s = state.getConnection().createStatement()) {
            int timeoutSeconds = Integer.getInteger("sqlancer.jdbc.queryTimeoutSeconds", 600);
            if (timeoutSeconds > 0) {
                s.setQueryTimeout(timeoutSeconds);
            }
            s.execute(sql);
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

    private Set<String> executeAndGetRowDigests(String query, List<MySQLColumn> columns) throws SQLException {
        Set<String> rowDigests = new LinkedHashSet<>();
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);
        state.getState().logStatement(query);
        SQLancerResultSet rs = new SQLQueryAdapter(query, errors).executeAndGet(state);
        if (rs == null) {
            throw new SQLException("Query failed (null ResultSet): " + query);
        }
        try {
            while (rs.next()) {
                rowDigests.add(computeCurrentRowDigest(rs, columns));
            }
        } finally {
            rs.close();
        }
        return rowDigests;
    }

    private Set<String> removePresentRowDigests(String query, List<MySQLColumn> columns, Set<String> expectedDigests)
            throws SQLException {
        Set<String> missing = new LinkedHashSet<>(expectedDigests);
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);
        state.getState().logStatement(query);
        SQLancerResultSet rs = new SQLQueryAdapter(query, errors).executeAndGet(state);
        if (rs == null) {
            throw new SQLException("Query failed (null ResultSet): " + query);
        }
        try {
            while (rs.next() && !missing.isEmpty()) {
                missing.remove(computeCurrentRowDigest(rs, columns));
            }
        } finally {
            rs.close();
        }
        return missing;
    }

    private String computeCurrentRowDigest(SQLancerResultSet rs, List<MySQLColumn> columns) throws SQLException {
        MessageDigest digest = newRowDigest();
        for (int i = 1; i <= columns.size(); i++) {
            if (i > 1) {
                digest.update((byte) '|');
            }
            String val = rs.getString(i);
            if (val != null) {
                val = val.stripTrailing();
            }
            if (isFloatingPointColumn(columns.get(i - 1))) {
                val = normalizeNumericValue(val);
            }
            String normalized = val == null ? "NULL" : val;
            digest.update(normalized.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        return toHex(digest.digest());
    }

    private static MessageDigest newRowDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >>> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    private static boolean isFloatingPointColumn(MySQLColumn col) {
        switch (col.getType()) {
        case FLOAT:
        case DOUBLE:
            return true;
        default:
            return false;
        }
    }

    private static String normalizeNumericValue(String val) {
        if (val == null) {
            return null;
        }
        try {
            double d = Double.parseDouble(val);
            if (d == 0.0 && Double.doubleToRawLongBits(d) != 0L) {
                return val.substring(1);
            }
        } catch (NumberFormatException ignored) {
        }
        return val;
    }

    private static String getOptionalString(SQLancerResultSet rs, int index) throws SQLException {
        try {
            return rs.getString(index);
        } catch (SQLException e) {
            return null;
        }
    }

    private static String nullToEmpty(String val) {
        return val == null ? "null" : val;
    }

    private static String formatPlan(List<String> plan) {
        if (plan == null || plan.isEmpty()) {
            return "[]";
        }
        return plan.stream().collect(Collectors.joining(" | "));
    }

    private static void logPlan(String label, List<String> plan) {
        if (!VERBOSE) {
            return;
        }
        if (plan == null || plan.isEmpty()) {
            System.out.println("  " + label + ": []");
            return;
        }
        System.out.println("  " + label + ":");
        for (String row : plan) {
            System.out.println("    " + row);
        }
    }

    private static void log(String msg) {
        if (VERBOSE) {
            System.out.println(msg);
        }
    }

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

    @Override
    public Reproducer<MySQLGlobalState> getLastReproducer() {
        return null;
    }

    @Override
    public String getLastQueryString() {
        return lastQueryString;
    }
}
