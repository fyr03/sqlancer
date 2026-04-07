package sqlancer.mysql.oracle;

import java.sql.SQLException;
import java.sql.Statement;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
// import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
    private static final boolean PLAN_CHANGE_PROGRESS_LOG =
            Boolean.parseBoolean(System.getProperty("sqlancer.subset3.planChangeProgressLog", "true"));
    private static final long PLAN_CHANGE_PROGRESS_LOG_INTERVAL_MILLIS =
            Math.max(1L, Long.getLong("sqlancer.subset3.planChangeProgressLogIntervalSeconds", 60L)) * 1000L;
    private static final DateTimeFormatter PROGRESS_TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger(0);
    private static final AtomicInteger TOTAL_PLAN_COMPARISONS = new AtomicInteger(0);
    private static final AtomicInteger TOTAL_PLAN_CHANGES = new AtomicInteger(0);
    private static final AtomicInteger LAST_LOGGED_PLAN_COMPARISONS = new AtomicInteger(0);
    private static final AtomicInteger LAST_LOGGED_PLAN_CHANGES = new AtomicInteger(0);
    private static final AtomicLong LAST_PROGRESS_LOG_MILLIS = new AtomicLong(System.currentTimeMillis());
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

    private enum QueryShape {
        PLAIN_SELECT,
        COUNT_STAR,
        GROUP_BY_COUNT,
        GROUP_BY_MAX,
        ORDER_BY_LIMIT_ASC,
        ORDER_BY_LIMIT_DESC,
        COUNT_DISTINCT,
        MULTI_PREDICATE
    }

    private static final class QuerySpec {
        private final String tableName;
        private final QueryShape shape;
        private final String whereClause;
        private final String selectQuery;
        private final List<MySQLColumn> resultColumns;
        private final MySQLColumn predicateColumn;
        private final MySQLColumn groupingColumn;

        private QuerySpec(String tableName, QueryShape shape, String whereClause, String selectQuery,
                List<MySQLColumn> resultColumns, MySQLColumn predicateColumn, MySQLColumn groupingColumn) {
            this.tableName = tableName;
            this.shape = shape;
            this.whereClause = whereClause;
            this.selectQuery = selectQuery;
            this.resultColumns = resultColumns;
            this.predicateColumn = predicateColumn;
            this.groupingColumn = groupingColumn;
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

        private String getGroupedCountQuery() {
            if (groupingColumn == null) {
                throw new AssertionError("groupingColumn is null for " + shape);
            }
            return "SELECT `" + groupingColumn.getName() + "`, COUNT(*) FROM " + tableName + whereClause
                    + " GROUP BY `" + groupingColumn.getName() + "`";
        }
    }

    private static final class QuerySnapshot {
        private final Long count;
        private final Map<String, Integer> rowDigests;
        private final Map<String, Double> maxValues;
        private final Map<String, Double> minValues;
        private final Map<String, Long> groupedCounts;
        private final Map<String, Double> groupedMaxValues;
        private final Long distinctCount;
        private final Double orderedValue;
        private final List<String> plan;

        private QuerySnapshot(Long count, Map<String, Integer> rowDigests, Map<String, Double> maxValues,
                Map<String, Double> minValues, Map<String, Long> groupedCounts,
                Map<String, Double> groupedMaxValues, Long distinctCount, Double orderedValue, List<String> plan) {
            this.count = count;
            this.rowDigests = rowDigests;
            this.maxValues = maxValues;
            this.minValues = minValues;
            this.groupedCounts = groupedCounts;
            this.groupedMaxValues = groupedMaxValues;
            this.distinctCount = distinctCount;
            this.orderedValue = orderedValue;
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
        private final String expansionHotValue;
        private final PredicateShiftMode shiftMode;
        private final Map<String, List<String>> hotValuesByColumn;

        private SkewProfile(MySQLColumn predicateColumn, String primaryHotValue, String secondaryHotValue,
                String tertiaryHotValue, String expansionHotValue, PredicateShiftMode shiftMode,
                Map<String, List<String>> hotValuesByColumn) {
            this.predicateColumn = predicateColumn;
            this.primaryHotValue = primaryHotValue;
            this.secondaryHotValue = secondaryHotValue;
            this.tertiaryHotValue = tertiaryHotValue;
            this.expansionHotValue = expansionHotValue;
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
                log("  Baseline[" + (i + 1) + "] " + summarizeSnapshot(baseline.query, baseline.snapshot));
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
                recordPlanComparison(planChanged);
                logPlanComparison(i + 1, baseline.query.selectQuery, baseline.snapshot.plan, s2Plan, planChanged);
                log("  Plan changed after ANALYZE TABLE [" + (i + 1) + "]: " + planChanged);
                if (!planChanged) {
                    boolean sampled = Randomly.getPercentage() < UNCHANGED_PLAN_VERIFICATION_PROBABILITY;
                    log("  Plan unchanged sampling decision [" + (i + 1) + "]: "
                            + (sampled ? "verify" : "skip"));
                    if (!sampled) {
                        continue;
                    }
                }
                QuerySnapshot s2Snapshot = executeSnapshot("S2", baseline.query, numericCols, false, s2Plan);
                try {
                    log("  S2[" + (i + 1) + "] " + summarizeSnapshot(baseline.query, s2Snapshot));
                    suppressKnownReportedBug(table, baseline.query, baseline.snapshot, s2Snapshot);

                log("\n[Step 6] Checking monotonicity across S1 ⊆ S2...");
                log("\n[Step 6." + (i + 1) + "] Checking monotonicity across S1/S2...");
                    verifyQueryShape(baseline.query, baseline.snapshot, s2Snapshot, numericCols);
                } catch (IgnoreMeException e) {
                    log("  Baseline[" + (i + 1) + "] skipped: "
                            + (e.getMessage() == null ? "suppressed known bug pattern" : e.getMessage()));
                }
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
            QuerySpec candidate = buildQuerySpec(table, numericCols, skewProfile);
            if (phases.containsKey(candidate.selectQuery)) {
                continue;
            }
            try {
                QuerySnapshot snapshot = executeSnapshot("S1", candidate, numericCols, true);
                if (!hasUsefulBaselineResult(candidate, snapshot)) {
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

    private QuerySpec buildQuerySpec(MySQLTable table, List<MySQLColumn> numericCols, SkewProfile skewProfile) {
        QueryShape shape = chooseQueryShape(table, numericCols);
        String whereClause = buildWhereClause(skewProfile);
        MySQLColumn groupingColumn = null;
        MySQLColumn aggregateColumn = null;
        List<MySQLColumn> resultColumns = new ArrayList<>();
        String selectQuery;
        switch (shape) {
        case COUNT_STAR:
            selectQuery = "SELECT COUNT(*) FROM " + table.getName() + whereClause;
            break;
        case GROUP_BY_COUNT:
            groupingColumn = chooseGroupingColumn(table);
            selectQuery = "SELECT `" + groupingColumn.getName() + "`, COUNT(*) FROM "
                    + table.getName() + whereClause + " GROUP BY `" + groupingColumn.getName() + "`";
            break;
        case GROUP_BY_MAX:
            groupingColumn = chooseGroupingColumn(table);
            aggregateColumn = chooseAggregateColumn(numericCols);
            selectQuery = "SELECT `" + groupingColumn.getName() + "`, MAX(`" + aggregateColumn.getName() + "`) FROM "
                    + table.getName() + whereClause + " GROUP BY `" + groupingColumn.getName() + "`";
            break;
        case ORDER_BY_LIMIT_ASC:
            aggregateColumn = chooseAggregateColumn(numericCols);
            whereClause = appendConjunct(whereClause, "`" + aggregateColumn.getName() + "` IS NOT NULL");
            selectQuery = "SELECT `" + aggregateColumn.getName() + "` FROM " + table.getName()
                    + whereClause
                    + " ORDER BY `" + aggregateColumn.getName() + "` ASC LIMIT 1";
            break;
        case ORDER_BY_LIMIT_DESC:
            aggregateColumn = chooseAggregateColumn(numericCols);
            whereClause = appendConjunct(whereClause, "`" + aggregateColumn.getName() + "` IS NOT NULL");
            selectQuery = "SELECT `" + aggregateColumn.getName() + "` FROM " + table.getName()
                    + whereClause
                    + " ORDER BY `" + aggregateColumn.getName() + "` DESC LIMIT 1";
            break;
        case COUNT_DISTINCT:
            aggregateColumn = chooseDistinctColumn(table);
            selectQuery = "SELECT COUNT(DISTINCT `" + aggregateColumn.getName() + "`) FROM "
                    + table.getName() + whereClause;
            break;
        case MULTI_PREDICATE:
            MySQLColumn secondaryPredicateColumn = chooseSecondaryPredicateColumn(table, skewProfile.predicateColumn);
            whereClause = appendConjunct(whereClause, "`" + secondaryPredicateColumn.getName() + "` IS NOT NULL");
            resultColumns = chooseProjectionColumns(table, skewProfile.predicateColumn);
            selectQuery = buildPlainSelectQuery(table, whereClause, resultColumns);
            break;
        case PLAIN_SELECT:
        default:
            resultColumns = chooseProjectionColumns(table, skewProfile.predicateColumn);
            selectQuery = buildPlainSelectQuery(table, whereClause, resultColumns);
            break;
        }
        logSQL(selectQuery);
        return new QuerySpec(table.getName(), shape, whereClause, selectQuery, resultColumns,
                skewProfile.predicateColumn, groupingColumn);
    }

    private boolean hasUsefulBaselineResult(QuerySpec query, QuerySnapshot snapshot) {
        switch (query.shape) {
        case COUNT_STAR:
        case PLAIN_SELECT:
        case MULTI_PREDICATE:
            return snapshot.count != null && snapshot.count > 0L;
        case GROUP_BY_COUNT:
            return snapshot.groupedCounts != null && !snapshot.groupedCounts.isEmpty();
        case GROUP_BY_MAX:
            return snapshot.groupedMaxValues != null && !snapshot.groupedMaxValues.isEmpty();
        case ORDER_BY_LIMIT_ASC:
        case ORDER_BY_LIMIT_DESC:
            return snapshot.orderedValue != null;
        case COUNT_DISTINCT:
            return snapshot.distinctCount != null && snapshot.distinctCount > 0L;
        default:
            throw new AssertionError(query.shape);
        }
    }

    private QueryShape chooseQueryShape(MySQLTable table, List<MySQLColumn> numericCols) {
        List<QueryShape> shapes = new ArrayList<>();
        shapes.add(QueryShape.PLAIN_SELECT);
        shapes.add(QueryShape.COUNT_STAR);
        shapes.add(QueryShape.GROUP_BY_COUNT);
        shapes.add(QueryShape.COUNT_DISTINCT);
        if (!numericCols.isEmpty()) {
            shapes.add(QueryShape.GROUP_BY_MAX);
            shapes.add(QueryShape.ORDER_BY_LIMIT_ASC);
            shapes.add(QueryShape.ORDER_BY_LIMIT_DESC);
        }
        if (table.getColumns().size() > 1) {
            shapes.add(QueryShape.MULTI_PREDICATE);
        }
        return Randomly.fromList(shapes);
    }

    private String buildPlainSelectQuery(MySQLTable table, String whereClause, List<MySQLColumn> projectionColumns) {
        String selectColumns = projectionColumns.stream()
                .map(c -> "`" + c.getName() + "`")
                .collect(Collectors.joining(", "));
        return "SELECT " + selectColumns + " FROM " + table.getName() + whereClause;
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
        switch (Randomly.fromOptions(0, 1, 2, 3, 4, 5, 6, 7)) {
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
        case 5:
            return " WHERE " + col + " = " + skewProfile.expansionHotValue;
        case 6:
            return " WHERE " + col + " IN (" + skewProfile.expansionHotValue + ", " + skewProfile.tertiaryHotValue
                    + ")";
        default:
            return " WHERE " + col + " IS NULL";
        }
    }

    private String buildVarcharWhereClause(MySQLColumn predicate, SkewProfile skewProfile) {
        String col = "`" + predicate.getName() + "`";
        String primary = skewProfile.primaryHotValue;
        String secondary = skewProfile.secondaryHotValue;
        String expansion = skewProfile.expansionHotValue;
        String primaryLiteral = unquoteLiteral(primary);
        String prefix = primaryLiteral.isEmpty() ? "h" : primaryLiteral.substring(0, 1);
        switch (Randomly.fromOptions(0, 1, 2, 3, 4, 5, 6)) {
        case 0:
            return " WHERE " + col + " = " + primary;
        case 1:
            return " WHERE " + col + " IN (" + primary + ", " + secondary + ")";
        case 2:
            return " WHERE " + col + " LIKE '" + prefix + "%'";
        case 3:
            return " WHERE " + col + " >= " + primary;
        case 4:
            return " WHERE " + col + " = " + expansion;
        case 5:
            return " WHERE " + col + " IN (" + expansion + ", " + secondary + ")";
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

    private MySQLColumn chooseGroupingColumn(MySQLTable table) {
        List<MySQLColumn> nonPrimaryCols = table.getColumns().stream()
                .filter(c -> !c.isPrimaryKey())
                .filter(c -> c.getType() != sqlancer.mysql.MySQLSchema.MySQLDataType.FLOAT)
                .filter(c -> c.getType() != sqlancer.mysql.MySQLSchema.MySQLDataType.DOUBLE)
                .collect(Collectors.toList());
        if (!nonPrimaryCols.isEmpty()) {
            return Randomly.fromList(nonPrimaryCols);
        }
        List<MySQLColumn> safeFallback = table.getColumns().stream()
                .filter(c -> c.getType() != sqlancer.mysql.MySQLSchema.MySQLDataType.FLOAT)
                .filter(c -> c.getType() != sqlancer.mysql.MySQLSchema.MySQLDataType.DOUBLE)
                .collect(Collectors.toList());
        if (!safeFallback.isEmpty()) {
            return Randomly.fromList(safeFallback);
        }
        throw new IgnoreMeException();
    }

    private MySQLColumn chooseAggregateColumn(List<MySQLColumn> numericCols) {
        if (numericCols.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(numericCols);
    }

    private MySQLColumn chooseDistinctColumn(MySQLTable table) {
        return Randomly.fromList(table.getColumns());
    }

    private MySQLColumn chooseSecondaryPredicateColumn(MySQLTable table, MySQLColumn predicateColumn) {
        List<MySQLColumn> otherColumns = table.getColumns().stream()
                .filter(c -> !c.equals(predicateColumn))
                .collect(Collectors.toList());
        if (otherColumns.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(otherColumns);
    }

    private String appendConjunct(String whereClause, String predicate) {
        return whereClause + " AND " + predicate;
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
        Map<String, Double> maxValues = new LinkedHashMap<>();
        Map<String, Double> minValues = new LinkedHashMap<>();
        Map<String, Long> groupedCounts = new LinkedHashMap<>();
        Map<String, Double> groupedMaxValues = new LinkedHashMap<>();
        Long count = null;
        Map<String, Integer> rowDigests = null;
        Long distinctCount = null;
        Double orderedValue = null;

        switch (query.shape) {
        case PLAIN_SELECT:
        case MULTI_PREDICATE:
            count = executeSingleLong(query.getCountQuery());
            rowDigests = captureRows ? executeAndGetRowDigests(query.selectQuery, query.resultColumns) : null;
            for (MySQLColumn col : numericCols) {
                maxValues.put(col.getName(), executeSingleDouble(query.getMaxQuery(col.getName())));
                minValues.put(col.getName(), executeSingleDouble(query.getMinQuery(col.getName())));
            }
            break;
        case COUNT_STAR:
            count = executeSingleLong(query.selectQuery);
            break;
        case GROUP_BY_COUNT:
            groupedCounts = executeGroupedLongMap(query.selectQuery, query.groupingColumn);
            break;
        case GROUP_BY_MAX:
            groupedMaxValues = executeGroupedDoubleMap(query.selectQuery, query.groupingColumn);
            groupedCounts = executeGroupedLongMap(query.getGroupedCountQuery(), query.groupingColumn);
            break;
        case ORDER_BY_LIMIT_ASC:
        case ORDER_BY_LIMIT_DESC:
            orderedValue = executeSingleDouble(query.selectQuery);
            break;
        case COUNT_DISTINCT:
            distinctCount = executeSingleLong(query.selectQuery);
            break;
        default:
            throw new AssertionError(query.shape);
        }
        List<String> plan = precomputedPlan != null ? precomputedPlan : captureExplainPlan(query.selectQuery);
        if (rowDigests != null) {
            log("  " + label + " result rows: " + totalDigestCount(rowDigests));
        }
        return new QuerySnapshot(count, rowDigests, maxValues, minValues, groupedCounts, groupedMaxValues,
                distinctCount, orderedValue, plan);
    }

    private void suppressKnownReportedBug(MySQLTable table, QuerySpec query, QuerySnapshot s1, QuerySnapshot s2)
            throws Exception {
        if (!SUPPRESS_KNOWN_FLOAT_ZERO_MAX_NULL_BUG) {
            return;
        }
        String numericPredicateDescription = extractNumericPredicateDescription(query);
        if (numericPredicateDescription == null) {
            return;
        }
        String affectedColumn = findKnownNumericMaxNullBugColumn(table, query, s1, s2);
        if (affectedColumn == null) {
            return;
        }
        log("  Known reported MySQL bug pattern detected (numeric range/equality index path leaks NULL MAX).");
        log("  Numeric predicate: " + numericPredicateDescription + ", affected column: " + affectedColumn);
        log("  Suppressing this round so Oracle3 can continue searching for new bugs.");
        throw new IgnoreMeException();
    }

    private String extractNumericPredicateDescription(QuerySpec query) {
        if (query.predicateColumn == null || !query.predicateColumn.getType().isNumeric()) {
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

    private String findKnownNumericMaxNullBugColumn(MySQLTable table, QuerySpec query, QuerySnapshot s1, QuerySnapshot s2)
            throws SQLException {
        if (query.shape == QueryShape.GROUP_BY_MAX) {
            return findKnownNumericGroupedMaxNullBugColumn(table, query, s1, s2);
        }
        if (s1.count == null || s1.count <= 0 || s2.count == null || s2.count <= 0) {
            return null;
        }
        MySQLTable currentTable = findTable(query.tableName);
        MySQLTable tableWithFreshIndexes = currentTable != null ? currentTable : table;
        for (MySQLColumn col : tableWithFreshIndexes.getColumns()) {
            if (!col.getType().isNumeric()) {
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

    private String findKnownNumericGroupedMaxNullBugColumn(MySQLTable table, QuerySpec query, QuerySnapshot s1,
            QuerySnapshot s2) throws SQLException {
        if (query.groupingColumn == null || s1.groupedMaxValues == null || s1.groupedMaxValues.isEmpty()
                || s2.groupedMaxValues == null) {
            return null;
        }

        String aggregateColumn = extractGroupedMaxAggregateColumn(query);
        if (aggregateColumn == null) {
            return null;
        }

        MySQLTable currentTable = findTable(query.tableName);
        MySQLTable tableWithFreshIndexes = currentTable != null ? currentTable : table;
        MySQLColumn aggregateColumnRef = tableWithFreshIndexes.getColumns().stream()
                .filter(c -> c.getName().equalsIgnoreCase(aggregateColumn))
                .findFirst()
                .orElse(null);
        if (aggregateColumnRef == null || !aggregateColumnRef.getType().isNumeric()) {
            return null;
        }

        for (Map.Entry<String, Double> entry : s1.groupedMaxValues.entrySet()) {
            Double s1Value = entry.getValue();
            Double s2Value = s2.groupedMaxValues.get(entry.getKey());
            if (s1Value == null || s2Value != null) {
                continue;
            }
            Double ignoreIndexValue = executeSingleDouble(buildIgnoreIndexGroupedAggregateQuery(
                    tableWithFreshIndexes, query, aggregateColumn, entry.getKey()));
            if (ignoreIndexValue != null && ignoreIndexValue + 1e-9 >= s1Value) {
                return aggregateColumn;
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

    // private String buildIgnoreIndexSelectQuery(String selectQuery, String tableName) {
    //     String ignoreIndexClause = fetchCurrentIndexNames(tableName).stream()
    //             .collect(Collectors.joining(", "));
    //     if (ignoreIndexClause.isEmpty()) {
    //         return selectQuery;
    //     }
    //     String fromNeedle = " FROM " + tableName;
    //     String replacement = fromNeedle + " IGNORE INDEX (" + ignoreIndexClause + ")";
    //     return selectQuery.replaceFirst(java.util.regex.Pattern.quote(fromNeedle),
    //             java.util.regex.Matcher.quoteReplacement(replacement));
    // }

    private String buildIgnoreIndexGroupedAggregateQuery(MySQLTable table, QuerySpec query, String aggregateColumn,
            String groupKey) {
        String ignoreIndexClause = fetchCurrentIndexNames(query.tableName).stream()
                .collect(Collectors.joining(", "));
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT MAX(`").append(aggregateColumn).append("`) FROM ").append(query.tableName);
        if (!ignoreIndexClause.isEmpty()) {
            sb.append(" IGNORE INDEX (").append(ignoreIndexClause).append(")");
        }
        sb.append(query.whereClause);
        sb.append(" AND `").append(query.groupingColumn.getName()).append("` <=> ")
                .append(toGroupKeyLiteral(groupKey, query.groupingColumn));
        return sb.toString();
    }

    private String extractGroupedMaxAggregateColumn(QuerySpec query) {
        if (query.shape != QueryShape.GROUP_BY_MAX) {
            return null;
        }
        String marker = "MAX(`";
        int start = query.selectQuery.indexOf(marker);
        if (start < 0) {
            return null;
        }
        start += marker.length();
        int end = query.selectQuery.indexOf("`)", start);
        if (end < 0) {
            return null;
        }
        return query.selectQuery.substring(start, end);
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

    private void verifyCountDistinct(QuerySpec query, QuerySnapshot s1, QuerySnapshot s2) {
        if (s1.distinctCount == null || s2.distinctCount == null) {
            log("  COUNT(DISTINCT) skipped because one result was null");
            return;
        }
        boolean pass = s1.distinctCount <= s2.distinctCount;
        log(String.format("  COUNT(DISTINCT) S1=%-6s <= S2=%-6s   [%s]",
                s1.distinctCount, s2.distinctCount, pass ? "PASS" : "FAIL"));
        if (!pass) {
            lastQueryString = query.selectQuery;
            throw new AssertionError(String.format(
                    "Oracle3 COUNT DISTINCT violation: S1=%d > S2=%d%n"
                    + "  Query: %s%n  Plan1: %s%n  Plan2: %s",
                    s1.distinctCount, s2.distinctCount, query.selectQuery, formatPlan(s1.plan), formatPlan(s2.plan)));
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
        Map<String, Integer> missing = removePresentRowDigests(query.selectQuery, query.resultColumns, s1.rowDigests);
        boolean pass = missing.isEmpty();
        log(String.format("  ROW-SET    |S1|=%-6d subset |S2|=%-6d   [%s]",
                totalDigestCount(s1.rowDigests), s2.count, pass ? "PASS" : "FAIL"));
        if (!pass) {
            lastQueryString = query.selectQuery;
            throw new AssertionError(String.format(
                    "Oracle3 row-set subset violation: Res1 is not a subset of Res2%n"
                    + "  Missing row digests: %s%n  Query: %s%n  Plan1: %s%n  Plan2: %s",
                    missing, query.selectQuery, formatPlan(s1.plan), formatPlan(s2.plan)));
        }
    }

    private void verifyGroupedCount(QuerySpec query, QuerySnapshot s1, QuerySnapshot s2) {
        for (Map.Entry<String, Long> entry : s1.groupedCounts.entrySet()) {
            String groupKey = entry.getKey();
            Long s1Count = entry.getValue();
            Long s2Count = s2.groupedCounts.get(groupKey);
            boolean pass = s2Count != null && s1Count <= s2Count;
            log(String.format("  GROUP COUNT key=%s S1=%-6s <= S2=%-6s   [%s]",
                    groupKey, s1Count, s2Count, pass ? "PASS" : "FAIL"));
            if (!pass) {
                lastQueryString = query.selectQuery;
                throw new AssertionError(String.format(
                        "Oracle3 GROUP BY COUNT violation for key %s: S1=%s > S2=%s%n"
                        + "  Query: %s%n  Plan1: %s%n  Plan2: %s",
                        groupKey, s1Count, s2Count, query.selectQuery, formatPlan(s1.plan), formatPlan(s2.plan)));
            }
        }
    }

    private void verifyGroupedMax(QuerySpec query, QuerySnapshot s1, QuerySnapshot s2) {
        for (Map.Entry<String, Double> entry : s1.groupedMaxValues.entrySet()) {
            String groupKey = entry.getKey();
            Double s1Value = entry.getValue();
            Double s2Value = s2.groupedMaxValues.get(groupKey);
            if (s1Value == null) {
                log(String.format("  GROUP MAX key=%s skipped because S1 is NULL", groupKey));
                continue;
            }
            boolean pass = s2Value != null && s1Value <= s2Value + 1e-9;
            log(String.format("  GROUP MAX key=%s S1=%-12s <= S2=%-12s   [%s]",
                    groupKey, s1Value, s2Value, pass ? "PASS" : "FAIL"));
            if (!pass) {
                lastQueryString = query.selectQuery;
                throw new AssertionError(String.format(
                        "Oracle3 GROUP BY MAX violation for key %s: S1=%s > S2=%s%n"
                        + "  Query: %s%n  Plan1: %s%n  Plan2: %s",
                        groupKey, s1Value, s2Value, query.selectQuery, formatPlan(s1.plan), formatPlan(s2.plan)));
            }
        }
    }

    private void verifyOrderByLimitAsc(QuerySpec query, QuerySnapshot s1, QuerySnapshot s2) {
        if (s1.orderedValue == null || s2.orderedValue == null) {
            log("  ORDER BY ASC LIMIT 1 skipped because one result was null");
            return;
        }
        boolean pass = s2.orderedValue <= s1.orderedValue + 1e-9;
        log(String.format("  ORDER MIN  S1=%-12s >= S2=%-12s   [%s]",
                s1.orderedValue, s2.orderedValue, pass ? "PASS" : "FAIL"));
        if (!pass) {
            lastQueryString = query.selectQuery;
            throw new AssertionError(String.format(
                    "Oracle3 ORDER BY ASC LIMIT 1 violation: S1=%s < S2=%s%n"
                    + "  Query: %s%n  Plan1: %s%n  Plan2: %s",
                    s1.orderedValue, s2.orderedValue, query.selectQuery, formatPlan(s1.plan), formatPlan(s2.plan)));
        }
    }

    private void verifyOrderByLimitDesc(QuerySpec query, QuerySnapshot s1, QuerySnapshot s2) {
        if (s1.orderedValue == null || s2.orderedValue == null) {
            log("  ORDER BY DESC LIMIT 1 skipped because one result was null");
            return;
        }
        boolean pass = s1.orderedValue <= s2.orderedValue + 1e-9;
        log(String.format("  ORDER MAX  S1=%-12s <= S2=%-12s   [%s]",
                s1.orderedValue, s2.orderedValue, pass ? "PASS" : "FAIL"));
        if (!pass) {
            lastQueryString = query.selectQuery;
            throw new AssertionError(String.format(
                    "Oracle3 ORDER BY DESC LIMIT 1 violation: S1=%s > S2=%s%n"
                    + "  Query: %s%n  Plan1: %s%n  Plan2: %s",
                    s1.orderedValue, s2.orderedValue, query.selectQuery, formatPlan(s1.plan), formatPlan(s2.plan)));
        }
    }

    private void verifyQueryShape(QuerySpec query, QuerySnapshot s1, QuerySnapshot s2, List<MySQLColumn> numericCols)
            throws SQLException {
        switch (query.shape) {
        case PLAIN_SELECT:
        case MULTI_PREDICATE:
            verifyCount(query, s1, s2);
            for (MySQLColumn col : numericCols) {
                verifyMax(query, col.getName(), s1, s2);
                verifyMin(query, col.getName(), s1, s2);
            }
            verifySelectSubset(query, s1, s2);
            break;
        case COUNT_STAR:
            verifyCount(query, s1, s2);
            break;
        case GROUP_BY_COUNT:
            verifyGroupedCount(query, s1, s2);
            break;
        case GROUP_BY_MAX:
            verifyGroupedCount(query, s1, s2);
            verifyGroupedMax(query, s1, s2);
            break;
        case ORDER_BY_LIMIT_ASC:
            verifyOrderByLimitAsc(query, s1, s2);
            break;
        case ORDER_BY_LIMIT_DESC:
            verifyOrderByLimitDesc(query, s1, s2);
            break;
        case COUNT_DISTINCT:
            verifyCountDistinct(query, s1, s2);
            break;
        default:
            throw new AssertionError(query.shape);
        }
    }

    private String summarizeSnapshot(QuerySpec query, QuerySnapshot snapshot) {
        switch (query.shape) {
        case PLAIN_SELECT:
        case MULTI_PREDICATE:
        case COUNT_STAR:
            return "shape=" + query.shape + ", count=" + snapshot.count;
        case GROUP_BY_COUNT:
            return "shape=" + query.shape + ", groups=" + snapshot.groupedCounts.size();
        case GROUP_BY_MAX:
            return "shape=" + query.shape + ", groups=" + snapshot.groupedMaxValues.size();
        case ORDER_BY_LIMIT_ASC:
        case ORDER_BY_LIMIT_DESC:
            return "shape=" + query.shape + ", value=" + snapshot.orderedValue;
        case COUNT_DISTINCT:
            return "shape=" + query.shape + ", distinct=" + snapshot.distinctCount;
        default:
            return "shape=" + query.shape;
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
        String expansionHotValue = createExpansionHotValue(predicateColumn, predicateHotValues);
        PredicateShiftMode shiftMode = Randomly.fromOptions(PredicateShiftMode.values());
        return new SkewProfile(predicateColumn, primaryHotValue, secondaryHotValue, tertiaryHotValue,
                expansionHotValue, shiftMode, hotValuesByColumn);
    }

    private String createExpansionHotValue(MySQLColumn col, List<String> existingValues) {
        switch (col.getType()) {
        case INT: {
            int maxExisting = existingValues.stream()
                    .mapToInt(Integer::parseInt)
                    .max()
                    .orElse(0);
            for (int i = 0; i < 8; i++) {
                String candidate = String.valueOf(maxExisting + 20 + i);
                if (!existingValues.contains(candidate)) {
                    return candidate;
                }
            }
            return String.valueOf(maxExisting + 40);
        }
        case FLOAT:
        case DOUBLE: {
            double maxExisting = existingValues.stream()
                    .mapToDouble(Double::parseDouble)
                    .max()
                    .orElse(0.0);
            for (int i = 0; i < 8; i++) {
                String candidate = formatFloatingHotValue(maxExisting + 20.0 + i);
                if (!existingValues.contains(candidate)) {
                    return candidate;
                }
            }
            return formatFloatingHotValue(maxExisting + 40.0);
        }
        case DECIMAL: {
            double maxExisting = existingValues.stream()
                    .mapToDouble(Double::parseDouble)
                    .max()
                    .orElse(0.0);
            for (int i = 0; i < 8; i++) {
                String candidate = formatDecimalHotValue(maxExisting + 20.0 + i);
                if (!existingValues.contains(candidate)) {
                    return candidate;
                }
            }
            return formatDecimalHotValue(maxExisting + 40.0);
        }
        case VARCHAR:
            for (int i = 0; i < 8; i++) {
                String candidate = quoteStringLiteral("exp_" + Math.abs(state.getRandomly().getInteger()) + "_" + i);
                if (!existingValues.contains(candidate)) {
                    return candidate;
                }
            }
            for (int i = 0; i < 32; i++) {
                String candidate = quoteStringLiteral("exp_fallback_" + Math.abs(state.getRandomly().getInteger())
                        + "_" + i);
                if (!existingValues.contains(candidate)) {
                    return candidate;
                }
            }
            return quoteStringLiteral("exp_final_" + existingValues.size());
        default:
            return "NULL";
        }
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
            if (p < 0.63) {
                return skewProfile.primaryHotValue;
            } else if (p < 0.80) {
                return skewProfile.secondaryHotValue;
            } else if (p < 0.90) {
                return skewProfile.tertiaryHotValue;
            } else if (p < 0.94) {
                return skewProfile.expansionHotValue;
            }
            return MySQLVisitor.asString(gen.generateConstant());
        }
        switch (skewProfile.shiftMode) {
        case PRIMARY_HEAVY:
            if (p < 0.42) {
                return skewProfile.primaryHotValue;
            } else if (p < 0.63) {
                return skewProfile.secondaryHotValue;
            } else if (p < 0.75) {
                return skewProfile.tertiaryHotValue;
            } else if (p < 0.94) {
                return skewProfile.expansionHotValue;
            } else if (p < 0.98) {
                return "NULL";
            }
            return MySQLVisitor.asString(gen.generateConstant());
        case PRIMARY_SECONDARY_SPLIT:
            if (p < 0.28) {
                return skewProfile.primaryHotValue;
            } else if (p < 0.50) {
                return skewProfile.secondaryHotValue;
            } else if (p < 0.62) {
                return skewProfile.tertiaryHotValue;
            } else if (p < 0.94) {
                return skewProfile.expansionHotValue;
            } else if (p < 0.98) {
                return "NULL";
            }
            return MySQLVisitor.asString(gen.generateConstant());
        case SECONDARY_HEAVY:
            if (p < 0.14) {
                return skewProfile.primaryHotValue;
            } else if (p < 0.38) {
                return skewProfile.secondaryHotValue;
            } else if (p < 0.52) {
                return skewProfile.tertiaryHotValue;
            } else if (p < 0.93) {
                return skewProfile.expansionHotValue;
            } else if (p < 0.97) {
                return "NULL";
            }
            return MySQLVisitor.asString(gen.generateConstant());
        case NULL_HEAVY:
            if (p < 0.18) {
                return skewProfile.primaryHotValue;
            } else if (p < 0.30) {
                return skewProfile.secondaryHotValue;
            } else if (p < 0.38) {
                return skewProfile.tertiaryHotValue;
            } else if (p < 0.70) {
                return skewProfile.expansionHotValue;
            } else if (p < 0.90) {
                return "NULL";
            }
            return MySQLVisitor.asString(gen.generateConstant());
        case NOISE_HEAVY:
        default:
            if (p < 0.14) {
                return skewProfile.primaryHotValue;
            } else if (p < 0.24) {
                return skewProfile.secondaryHotValue;
            } else if (p < 0.32) {
                return skewProfile.tertiaryHotValue;
            } else if (p < 0.68) {
                return skewProfile.expansionHotValue;
            } else if (p < 0.76) {
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

    private Map<String, Long> executeGroupedLongMap(String query, MySQLColumn groupColumn) throws SQLException {
        Map<String, Long> groupedValues = new LinkedHashMap<>();
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);
        state.getState().logStatement(query);
        SQLancerResultSet rs = new SQLQueryAdapter(query, errors).executeAndGet(state);
        if (rs == null) {
            throw new SQLException("Query failed (null ResultSet): " + query);
        }
        try {
            while (rs.next()) {
                groupedValues.put(normalizeGroupKey(rs.getString(1), groupColumn), rs.getLong(2));
            }
        } finally {
            rs.close();
        }
        return groupedValues;
    }

    private Map<String, Double> executeGroupedDoubleMap(String query, MySQLColumn groupColumn) throws SQLException {
        Map<String, Double> groupedValues = new LinkedHashMap<>();
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);
        state.getState().logStatement(query);
        SQLancerResultSet rs = new SQLQueryAdapter(query, errors).executeAndGet(state);
        if (rs == null) {
            throw new SQLException("Query failed (null ResultSet): " + query);
        }
        try {
            while (rs.next()) {
                String rawValue = rs.getString(2);
                Double parsedValue;
                if (rawValue == null) {
                    parsedValue = null;
                } else {
                    try {
                        parsedValue = Double.parseDouble(rawValue);
                    } catch (NumberFormatException e) {
                        throw new IgnoreMeException();
                    }
                }
                groupedValues.put(normalizeGroupKey(rs.getString(1), groupColumn), parsedValue);
            }
        } finally {
            rs.close();
        }
        return groupedValues;
    }

    private Map<String, Integer> executeAndGetRowDigests(String query, List<MySQLColumn> columns)
            throws SQLException {
        Map<String, Integer> rowDigests = new LinkedHashMap<>();
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);
        state.getState().logStatement(query);
        SQLancerResultSet rs = new SQLQueryAdapter(query, errors).executeAndGet(state);
        if (rs == null) {
            throw new SQLException("Query failed (null ResultSet): " + query);
        }
        try {
            while (rs.next()) {
                String digest = computeCurrentRowDigest(rs, columns);
                rowDigests.merge(digest, 1, Integer::sum);
            }
        } finally {
            rs.close();
        }
        return rowDigests;
    }

    private Map<String, Integer> removePresentRowDigests(String query, List<MySQLColumn> columns,
            Map<String, Integer> expectedDigests) throws SQLException {
        Map<String, Integer> missing = new LinkedHashMap<>(expectedDigests);
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);
        state.getState().logStatement(query);
        SQLancerResultSet rs = new SQLQueryAdapter(query, errors).executeAndGet(state);
        if (rs == null) {
            throw new SQLException("Query failed (null ResultSet): " + query);
        }
        try {
            while (rs.next() && !missing.isEmpty()) {
                String digest = computeCurrentRowDigest(rs, columns);
                Integer count = missing.get(digest);
                if (count == null) {
                    continue;
                }
                if (count <= 1) {
                    missing.remove(digest);
                } else {
                    missing.put(digest, count - 1);
                }
            }
        } finally {
            rs.close();
        }
        return missing;
    }

    private int totalDigestCount(Map<String, Integer> digests) {
        if (digests == null) {
            return 0;
        }
        return digests.values().stream().mapToInt(Integer::intValue).sum();
    }

    private String normalizeGroupKey(String rawValue, MySQLColumn groupColumn) {
        if (rawValue == null) {
            return "NULL";
        }
        String normalized = rawValue.stripTrailing();
        if (groupColumn.getType() == sqlancer.mysql.MySQLSchema.MySQLDataType.DECIMAL) {
            try {
                return new BigDecimal(normalized).stripTrailingZeros().toPlainString();
            } catch (NumberFormatException ignored) {
                return normalized;
            }
        }
        if (isFloatingPointColumn(groupColumn)) {
            normalized = normalizeNumericValue(normalized);
        }
        return normalized;
    }

    private String toGroupKeyLiteral(String groupKey, MySQLColumn groupColumn) {
        if (groupKey == null || "NULL".equals(groupKey)) {
            return "NULL";
        }
        switch (groupColumn.getType()) {
        case VARCHAR:
            return quoteStringLiteral(groupKey);
        case INT:
        case DECIMAL:
        case FLOAT:
        case DOUBLE:
            return groupKey;
        default:
            throw new AssertionError(groupColumn.getType());
        }
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

    private static void recordPlanComparison(boolean planChanged) {
        TOTAL_PLAN_COMPARISONS.incrementAndGet();
        if (planChanged) {
            TOTAL_PLAN_CHANGES.incrementAndGet();
        }
        maybeLogPlanChangeProgress();
    }

    private static void maybeLogPlanChangeProgress() {
        if (!PLAN_CHANGE_PROGRESS_LOG) {
            return;
        }
        long now = System.currentTimeMillis();
        long lastLog = LAST_PROGRESS_LOG_MILLIS.get();
        if (now - lastLog < PLAN_CHANGE_PROGRESS_LOG_INTERVAL_MILLIS) {
            return;
        }
        if (!LAST_PROGRESS_LOG_MILLIS.compareAndSet(lastLog, now)) {
            return;
        }

        int totalComparisons = TOTAL_PLAN_COMPARISONS.get();
        int totalChanges = TOTAL_PLAN_CHANGES.get();
        int previousComparisons = LAST_LOGGED_PLAN_COMPARISONS.getAndSet(totalComparisons);
        int previousChanges = LAST_LOGGED_PLAN_CHANGES.getAndSet(totalChanges);

        double elapsedSeconds = Math.max(0.001d, (now - lastLog) / 1000d);
        int deltaComparisons = totalComparisons - previousComparisons;
        int deltaChanges = totalChanges - previousChanges;
        double recentChangeRate = deltaChanges / elapsedSeconds;
        double changeRatio = totalComparisons == 0 ? 0.0d : (100.0d * totalChanges / totalComparisons);

        System.out.println(String.format(Locale.US,
                "[%s] Oracle3 plan changes: %d/%d changed (%.2f%% overall, %.2f changes/s over last %.1fs).",
                LocalDateTime.now().format(PROGRESS_TIMESTAMP_FORMAT), totalChanges, totalComparisons,
                changeRatio, recentChangeRate, elapsedSeconds));

        if (VERBOSE) {
            log(String.format(Locale.US,
                    "  Oracle3 recent plan comparisons: %d, recent plan changes: %d",
                    deltaComparisons, deltaChanges));
        }
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
