package sqlancer.sqlite3.oracle;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
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
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.schema.SQLite3DataType;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

public class SQLite3SubsetOracle3 implements TestOracle<SQLite3GlobalState> {

    private static final boolean VERBOSE =
            Boolean.parseBoolean(System.getProperty("sqlancer.subset.verbose", "true"));

    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger(0);
    private static final int TARGET_BASELINE_QUERIES = 6;
    private static final int MIN_BASELINE_QUERIES = 3;
    private static final int MAX_QUERY_GENERATION_ATTEMPTS = 72;
    private static final int BASELINE_RANDOM_ROWS = 4;
    private static final int BASELINE_HOT_ROWS = 2;
    private static final int BASELINE_NOISE_ROWS = 4;
    private static final int SKEWED_EXPANSION_ROWS = 720;
    private static final double UNCHANGED_PLAN_VERIFICATION_PROBABILITY = 0.15;

    private final SQLite3GlobalState state;
    private final ExpectedErrors insertErrors;
    private String lastQueryString;

    private static final class QuerySpec {
        private final String tableName;
        private final String whereClause;
        private final String selectQuery;
        private final List<SQLite3Column> resultColumns;

        private QuerySpec(String tableName, String whereClause, String selectQuery, List<SQLite3Column> resultColumns) {
            this.tableName = tableName;
            this.whereClause = whereClause;
            this.selectQuery = selectQuery;
            this.resultColumns = resultColumns;
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
        private final SQLite3Column predicateColumn;
        private final String primaryHotValue;
        private final String secondaryHotValue;
        private final String tertiaryHotValue;
        private final PredicateShiftMode shiftMode;
        private final Map<String, List<String>> hotValuesByColumn;

        private SkewProfile(SQLite3Column predicateColumn, String primaryHotValue, String secondaryHotValue,
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

    public SQLite3SubsetOracle3(SQLite3GlobalState state) {
        this.state = state;
        this.insertErrors = new ExpectedErrors();
        SQLite3Errors.addInsertUpdateErrors(insertErrors);
        SQLite3Errors.addExpectedExpressionErrors(insertErrors);
        SQLite3Errors.addQueryErrors(insertErrors);
        insertErrors.add("[SQLITE_FULL]");
        insertErrors.add("cannot INSERT into generated column");
        insertErrors.add("The database file is locked");
    }

    @Override
    public void check() throws Exception {
        int id = TABLE_COUNTER.incrementAndGet();
        String tableName = "subset3" + id;

        log("================================================================");
        log(" SUBSET ORACLE 3 round #" + id);
        log("================================================================");

        try {
            log("\n[Step 1] Creating temporal table: " + tableName);
            SQLQueryAdapter createTable = createTemporalTable(tableName);
            logSQL(createTable.getQueryString());
            state.executeStatement(createTable);

            SQLite3Table table = findTable(tableName);
            if (table == null || table.isView() || table.isVirtual() || table.isReadOnly()) {
                throw new IgnoreMeException();
            }

            List<SQLite3Column> numericCols = table.getColumns().stream()
                    .filter(SQLite3SubsetOracle3::isNumericColumn)
                    .collect(Collectors.toList());
            log("  Created with columns: " + table.getColumns().stream()
                    .map(SQLite3Column::getName).collect(Collectors.toList()));

            SQLite3Column predicateColumn = choosePredicateColumn(table);
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

            log("\n[Step 4] Appending many skewed rows and running ANALYZE...");
            appendSkewedRows(table, skewProfile, SKEWED_EXPANSION_ROWS + 64 * Randomly.smallNumber(), 0.92,
                    DistributionStage.EXPANSION);

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
                log("  Plan changed after ANALYZE [" + (i + 1) + "]: " + planChanged);
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

                log("\n[Step 6." + (i + 1) + "] Checking monotonicity across S1/S2...");
                verifyCount(baseline.query, baseline.snapshot, s2Snapshot);
                for (SQLite3Column col : numericCols) {
                    verifyMax(baseline.query, col.getName(), baseline.snapshot, s2Snapshot);
                    verifyMin(baseline.query, col.getName(), baseline.snapshot, s2Snapshot);
                }
                verifySelectSubset(baseline.query, baseline.snapshot, s2Snapshot);
            }

            log("\n  All Oracle3 core checks PASSED for round #" + id);
        } catch (java.sql.SQLNonTransientConnectionException e) {
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

    private SQLQueryAdapter createTemporalTable(String tableName) {
        int nrPayloadColumns = 2 + Randomly.smallNumber();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName).append(" (");
        sb.append("id INTEGER PRIMARY KEY AUTOINCREMENT");
        for (int i = 0; i < nrPayloadColumns; i++) {
            sb.append(", c").append(i).append(" ").append(Randomly.fromOptions("INTEGER", "REAL", "TEXT"));
        }
        sb.append(")");
        ExpectedErrors errors = new ExpectedErrors();
        SQLite3Errors.addTableManipulationErrors(errors);
        errors.add("already exists");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private List<BaselinePhase> createValidatedBaselinePhases(SQLite3Table table, List<SQLite3Column> numericCols,
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

    private QuerySpec buildQuerySpec(SQLite3Table table, SkewProfile skewProfile) {
        String whereClause = buildWhereClause(skewProfile);
        List<SQLite3Column> projectionColumns = chooseProjectionColumns(table, skewProfile.predicateColumn);
        String selectColumns = projectionColumns.stream()
                .map(c -> "`" + c.getName() + "`")
                .collect(Collectors.joining(", "));
        String selectQuery = "SELECT " + selectColumns + " FROM " + table.getName() + whereClause;
        logSQL(selectQuery);
        return new QuerySpec(table.getName(), whereClause, selectQuery, projectionColumns);
    }

    private String buildWhereClause(SkewProfile skewProfile) {
        SQLite3Column predicate = skewProfile.predicateColumn;
        switch (predicate.getType()) {
        case INT:
        case REAL:
            return buildNumericWhereClause(predicate, skewProfile);
        case TEXT:
            return buildTextWhereClause(predicate, skewProfile);
        default:
            return " WHERE `" + predicate.getName() + "` = " + skewProfile.primaryHotValue;
        }
    }

    private String buildNumericWhereClause(SQLite3Column predicate, SkewProfile skewProfile) {
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

    private String buildTextWhereClause(SQLite3Column predicate, SkewProfile skewProfile) {
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
            return " WHERE " + col + " LIKE '" + escapeForSql(prefix) + "%'";
        case 3:
            return " WHERE " + col + " >= " + primary;
        default:
            return " WHERE " + col + " IS NULL";
        }
    }

    private List<SQLite3Column> chooseProjectionColumns(SQLite3Table table, SQLite3Column predicateColumn) {
        List<SQLite3Column> allColumns = table.getColumns().stream()
                .filter(this::supportsProjection)
                .collect(Collectors.toList());
        ProjectionStyle style = Randomly.fromOptions(ProjectionStyle.values());
        LinkedHashSet<SQLite3Column> chosen = new LinkedHashSet<>();
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
            chosen.addAll(Randomly.nonEmptySubset(allColumns).stream()
                    .limit(1 + Randomly.smallNumber())
                    .collect(Collectors.toList()));
            break;
        case NON_PREDICATE_FOCUS:
            List<SQLite3Column> nonPredicate = allColumns.stream()
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

    private QuerySnapshot executeSnapshot(String label, QuerySpec query, List<SQLite3Column> numericCols,
            boolean captureRows) throws Exception {
        return executeSnapshot(label, query, numericCols, captureRows, null);
    }

    private QuerySnapshot executeSnapshot(String label, QuerySpec query, List<SQLite3Column> numericCols,
            boolean captureRows, List<String> precomputedPlan) throws Exception {
        Long count = executeSingleLong(query.getCountQuery());
        Set<String> rowDigests = captureRows ? executeAndGetRowDigests(query.selectQuery, query.resultColumns) : null;
        Map<String, Double> maxValues = new LinkedHashMap<>();
        Map<String, Double> minValues = new LinkedHashMap<>();
        for (SQLite3Column col : numericCols) {
            maxValues.put(col.getName(), executeSingleDouble(query.getMaxQuery(col.getName())));
            minValues.put(col.getName(), executeSingleDouble(query.getMinQuery(col.getName())));
        }
        List<String> plan = precomputedPlan != null ? precomputedPlan : captureExplainPlan(query.selectQuery);
        if (rowDigests != null) {
            log("  " + label + " result rows: " + rowDigests.size());
        }
        return new QuerySnapshot(count, rowDigests, maxValues, minValues, plan);
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
                    "SQLite Oracle3 COUNT violation: COUNT(S1)=%d > COUNT(S2)=%d%n"
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
                    "SQLite Oracle3 MAX violation on %s: S1=%s > S2=%s%n"
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
                    "SQLite Oracle3 MIN violation on %s: S1=%s < S2=%s%n"
                    + "  Query: %s%n  Plan1: %s%n  Plan2: %s",
                    col, min1, min2, query.selectQuery, formatPlan(s1.plan), formatPlan(s2.plan)));
        }
    }

    private void verifySelectSubset(QuerySpec query, QuerySnapshot s1, QuerySnapshot s2) throws SQLException {
        Set<String> missing = removePresentRowDigests(query.selectQuery, query.resultColumns, s1.rowDigests);
        boolean pass = missing.isEmpty();
        log(String.format("  ROW-SET    |S1|=%-6d subset |S2|=%-6d   [%s]",
                s1.rowDigests.size(), s2.count == null ? 0L : s2.count, pass ? "PASS" : "FAIL"));
        if (!pass) {
            lastQueryString = query.selectQuery;
            throw new AssertionError(String.format(
                    "SQLite Oracle3 row-set subset violation: Res1 is not a subset of Res2%n"
                    + "  Missing row digests: %s%n  Query: %s%n  Plan1: %s%n  Plan2: %s",
                    missing, query.selectQuery, formatPlan(s1.plan), formatPlan(s2.plan)));
        }
    }

    private void appendHotSeedRows(SQLite3Table table, SkewProfile skewProfile, int nrRows) {
        for (int i = 0; i < nrRows; i++) {
            try {
                List<String> values = new ArrayList<>();
                for (SQLite3Column col : getInsertableColumns(table)) {
                    if (col.getName().equals(skewProfile.predicateColumn.getName())) {
                        values.add(skewProfile.primaryHotValue);
                    } else {
                        values.add(generateValue(col, skewProfile, 0.5, DistributionStage.BASELINE));
                    }
                }
                executeInsert(table, values);
            } catch (Throwable e) {
                log("  Hot seed INSERT skipped: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
    }

    private void appendSkewedRows(SQLite3Table table, SkewProfile skewProfile, int nrRows, double hotspotProbability,
            DistributionStage stage) {
        List<SQLite3Column> insertableColumns = getInsertableColumns(table);
        for (int i = 0; i < nrRows; i++) {
            try {
                List<String> values = insertableColumns.stream()
                        .map(c -> generateValue(c, skewProfile, hotspotProbability, stage))
                        .collect(Collectors.toList());
                executeInsert(table, values);
            } catch (Throwable e) {
                log("  Skewed INSERT skipped: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
    }

    private void analyzeTable(String tableName) throws Exception {
        String analyzeSql = "ANALYZE " + tableName;
        ExpectedErrors errors = new ExpectedErrors();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
        logSQL(analyzeSql);
        state.executeStatement(new SQLQueryAdapter(analyzeSql, errors));
    }

    private List<String> captureExplainPlan(String selectQuery) throws SQLException {
        String explainQuery = "EXPLAIN QUERY PLAN " + selectQuery;
        lastQueryString = explainQuery;
        ExpectedErrors errors = new ExpectedErrors();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
        state.getState().logStatement(explainQuery);
        SQLancerResultSet rs = new SQLQueryAdapter(explainQuery, errors).executeAndGet(state);
        if (rs == null) {
            throw new SQLException("EXPLAIN QUERY PLAN failed (null ResultSet): " + explainQuery);
        }
        try {
            List<String> planRows = new ArrayList<>();
            while (rs.next()) {
                planRows.add(String.format("id=%s;parent=%s;detail=%s",
                        nullToEmpty(getOptionalString(rs, "id")),
                        nullToEmpty(getOptionalString(rs, "parent")),
                        nullToEmpty(getOptionalString(rs, "detail"))));
            }
            return planRows;
        } finally {
            rs.close();
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
        if (row == null) {
            return "null";
        }
        return row.replaceAll("\\b\\d+\\b", "?")
                .replaceAll("\\s+", " ")
                .trim()
                .toLowerCase(Locale.ROOT);
    }

    private void insertNoiseRows(SQLite3Table table, int nrRows) {
        List<SQLite3Column> cols = getInsertableColumns(table);
        Map<SQLite3Column, List<String>> boundaryMap = new LinkedHashMap<>();
        for (SQLite3Column col : cols) {
            List<String> vals = new ArrayList<>();
            switch (col.getType()) {
            case INT:
                vals.add("0");
                vals.add("1");
                vals.add("-1");
                vals.add(Long.toString(Integer.MIN_VALUE));
                vals.add(Long.toString(Integer.MAX_VALUE));
                vals.add(Long.toString(Long.MIN_VALUE));
                vals.add(Long.toString(Long.MAX_VALUE));
                vals.add("NULL");
                break;
            case REAL:
                vals.add("0.0");
                vals.add("-0.0");
                vals.add("1.0");
                vals.add("-1.0");
                vals.add("3.4028235E38");
                vals.add("-3.4028235E38");
                vals.add("1.4E-45");
                vals.add("NULL");
                break;
            case TEXT:
                vals.add("''");
                vals.add("'%'");
                vals.add("'_'");
                vals.add("'NULL'");
                vals.add("'0'");
                vals.add("'" + "a".repeat(128) + "'");
                vals.add("NULL");
                break;
            default:
                vals.add("NULL");
                break;
            }
            boundaryMap.put(col, vals);
        }

        for (int i = 0; i < nrRows; i++) {
            SQLite3Column targetCol = Randomly.fromList(cols);
            String boundaryVal = Randomly.fromList(boundaryMap.get(targetCol));
            List<String> values = new ArrayList<>();
            for (SQLite3Column col : cols) {
                values.add(col.equals(targetCol) ? boundaryVal : "NULL");
            }
            try {
                executeInsert(table, values);
            } catch (Throwable e) {
                log("  Noise INSERT skipped: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
    }

    private SkewProfile createSkewProfile(SQLite3Table table, SQLite3Column predicateColumn) {
        Map<String, List<String>> hotValuesByColumn = new LinkedHashMap<>();
        for (SQLite3Column col : getInsertableColumns(table)) {
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

    private List<String> createHotValues(SQLite3Column col) {
        List<String> hotValues = new ArrayList<>();
        switch (col.getType()) {
        case INT:
            long intAnchor = state.getRandomly().getInteger(-16, 17);
            hotValues.add(Long.toString(intAnchor));
            hotValues.add(Long.toString(intAnchor + 1 + Randomly.smallNumber()));
            hotValues.add(Long.toString(intAnchor - 1 - Randomly.smallNumber()));
            break;
        case REAL:
            double floatAnchor = state.getRandomly().getInteger(-200, 201) / 10.0;
            hotValues.add(formatFloatingLiteral(floatAnchor));
            hotValues.add(formatFloatingLiteral(floatAnchor + 1.0 + Randomly.smallNumber()));
            hotValues.add(formatFloatingLiteral(floatAnchor - 1.0 - Randomly.smallNumber()));
            break;
        case TEXT:
            String stringStem = "hv" + Math.abs(state.getRandomly().getInteger());
            hotValues.add(quoteStringLiteral(stringStem));
            hotValues.add(quoteStringLiteral(stringStem + "a"));
            hotValues.add(quoteStringLiteral(stringStem + "b"));
            break;
        default:
            hotValues.add("NULL");
            break;
        }
        return hotValues;
    }

    private SQLite3Column choosePredicateColumn(SQLite3Table table) {
        List<SQLite3Column> preferredCols = table.getColumns().stream()
                .filter(c -> !c.isPrimaryKey())
                .filter(this::supportsPredicate)
                .collect(Collectors.toList());
        if (preferredCols.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(preferredCols);
    }

    private void ensureSupportingIndexes(SQLite3Table table, SQLite3Column predicateColumn, int id) {
        ExpectedErrors errors = new ExpectedErrors();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
        errors.add("already exists");
        errors.add("The database file is locked");

        String indexName = "i3p" + id;
        String createIndexSql = "CREATE INDEX " + indexName + " ON " + table.getName()
                + " (`" + predicateColumn.getName() + "`)";
        logSQL(createIndexSql);
        try {
            state.executeStatement(new SQLQueryAdapter(createIndexSql, errors, true));
        } catch (Throwable e) {
            log("  Supporting index creation skipped: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }

        List<SQLite3Column> secondaryCandidates = table.getColumns().stream()
                .filter(this::supportsProjection)
                .filter(c -> !c.equals(predicateColumn))
                .collect(Collectors.toList());
        if (secondaryCandidates.isEmpty() || Randomly.getBooleanWithSmallProbability()) {
            return;
        }
        SQLite3Column secondaryColumn = Randomly.fromList(secondaryCandidates);
        String compositeIndexName = "i3c" + id;
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
            state.executeStatement(new SQLQueryAdapter(compositeIndexSql, errors, true));
        } catch (Throwable e) {
            log("  Composite supporting index creation skipped: " + e.getClass().getSimpleName()
                    + ": " + e.getMessage());
        }
    }

    private String generateValue(SQLite3Column col, SkewProfile skewProfile, double hotspotProbability,
            DistributionStage stage) {
        if (col.getName().equals(skewProfile.predicateColumn.getName())) {
            return generatePredicateValue(col, skewProfile, stage);
        }
        List<String> hotValues = skewProfile.hotValuesByColumn.get(col.getName());
        if (hotValues != null && Randomly.getPercentage() < hotspotProbability) {
            return Randomly.fromList(hotValues);
        }
        return generateFallbackValue(col);
    }

    private String generatePredicateValue(SQLite3Column col, SkewProfile skewProfile, DistributionStage stage) {
        double p = Randomly.getPercentage();
        if (stage == DistributionStage.BASELINE) {
            if (p < 0.80) {
                return skewProfile.primaryHotValue;
            } else if (p < 0.92) {
                return skewProfile.secondaryHotValue;
            } else if (p < 0.97) {
                return skewProfile.tertiaryHotValue;
            }
            return generateFallbackValue(col);
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
            break;
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
            break;
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
            break;
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
            break;
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
            break;
        }
        return generateFallbackValue(col);
    }

    private String generateFallbackValue(SQLite3Column col) {
        switch (col.getType()) {
        case INT:
            return Long.toString(state.getRandomly().getInteger());
        case REAL:
            return formatFloatingLiteral(state.getRandomly().getDouble());
        case TEXT:
            return quoteStringLiteral(state.getRandomly().getString());
        default:
            return SQLite3Visitor.asString(sqlancer.sqlite3.gen.SQLite3ExpressionGenerator.getRandomLiteralValue(state));
        }
    }

    private String formatFloatingLiteral(double value) {
        if (!Double.isFinite(value)) {
            value = 0.0;
        }
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private String quoteStringLiteral(String value) {
        return "'" + escapeForSql(value) + "'";
    }

    private String escapeForSql(String value) {
        return value.replace("'", "''");
    }

    private void executeInsert(SQLite3Table table, List<String> values) throws Exception {
        List<SQLite3Column> insertableColumns = getInsertableColumns(table);
        StringBuilder sb = new StringBuilder("INSERT OR IGNORE INTO ");
        sb.append(table.getName()).append(" (");
        sb.append(insertableColumns.stream().map(c -> "`" + c.getName() + "`").collect(Collectors.joining(", ")));
        sb.append(") VALUES (");
        sb.append(String.join(", ", values));
        sb.append(")");
        logSQL(sb.toString());
        state.executeStatement(new SQLQueryAdapter(sb.toString(), insertErrors));
    }

    private List<SQLite3Column> getInsertableColumns(SQLite3Table table) {
        return table.getColumns().stream()
                .filter(c -> !c.isPrimaryKey())
                .collect(Collectors.toList());
    }

    private SQLite3Table findTable(String name) {
        return state.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getName().equalsIgnoreCase(name))
                .findFirst().orElse(null);
    }

    private void dropIfExists(String tableName) {
        try {
            state.executeStatement(new SQLQueryAdapter("DROP TABLE IF EXISTS " + tableName, true));
        } catch (Exception ignored) {
        }
    }

    private Long executeSingleLong(String query) throws SQLException {
        lastQueryString = query;
        ExpectedErrors errors = new ExpectedErrors();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
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
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
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

    private Set<String> executeAndGetRowDigests(String query, List<SQLite3Column> columns) throws SQLException {
        Set<String> rowDigests = new LinkedHashSet<>();
        ExpectedErrors errors = new ExpectedErrors();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
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

    private Set<String> removePresentRowDigests(String query, List<SQLite3Column> columns, Set<String> expectedDigests)
            throws SQLException {
        Set<String> missing = new LinkedHashSet<>(expectedDigests);
        ExpectedErrors errors = new ExpectedErrors();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
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

    private String computeCurrentRowDigest(SQLancerResultSet rs, List<SQLite3Column> columns) throws SQLException {
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
            digest.update((val == null ? "NULL" : val).getBytes(java.nio.charset.StandardCharsets.UTF_8));
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

    private boolean supportsPredicate(SQLite3Column col) {
        return col.getType() == SQLite3DataType.INT || col.getType() == SQLite3DataType.REAL
                || col.getType() == SQLite3DataType.TEXT;
    }

    private boolean supportsProjection(SQLite3Column col) {
        return col.getType() != SQLite3DataType.BINARY && col.getType() != SQLite3DataType.NONE;
    }

    private static boolean isFloatingPointColumn(SQLite3Column col) {
        return col.getType() == SQLite3DataType.REAL;
    }

    private static boolean isNumericColumn(SQLite3Column col) {
        return col.getType() == SQLite3DataType.INT || col.getType() == SQLite3DataType.REAL;
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

    private static String getOptionalString(SQLancerResultSet rs, String identifier) throws SQLException {
        try {
            return rs.getString(identifier);
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
        String trimmed = sql == null ? "(null)" : sql.trim().replace("\n", " ");
        if (trimmed.length() > 120) {
            trimmed = trimmed.substring(0, 117) + "...";
        }
        System.out.println("  SQL> " + trimmed);
    }

    @Override
    public Reproducer<SQLite3GlobalState> getLastReproducer() {
        return null;
    }

    @Override
    public String getLastQueryString() {
        return lastQueryString;
    }
}
