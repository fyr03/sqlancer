package sqlancer.mysql;

import java.sql.SQLException;
import java.util.Optional;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CERTOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.mysql.gen.MySQLExpressionGenerator;
import sqlancer.mysql.oracle.MySQLDQEOracle;
import sqlancer.mysql.oracle.MySQLDQPOracle;
import sqlancer.mysql.oracle.MySQLFuzzer;
import sqlancer.mysql.oracle.MySQLPivotedQuerySynthesisOracle;
import sqlancer.mysql.oracle.MySQLSubsetOracle;
import sqlancer.mysql.oracle.MySQLSubsetOracle2;
import sqlancer.mysql.oracle.MySQLSubsetOracle3;

public enum MySQLOracleFactory implements OracleFactory<MySQLGlobalState> {

    TLP_WHERE {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
            MySQLExpressionGenerator gen = new MySQLExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(MySQLErrors.getExpressionErrors())
                    .withRegex(MySQLErrors.getExpressionRegexErrors()).build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }

    },
    PQS {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
            return new MySQLPivotedQuerySynthesisOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }

    },
    CERT {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
            MySQLExpressionGenerator gen = new MySQLExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(MySQLErrors.getExpressionErrors())
                    .withRegex(MySQLErrors.getExpressionRegexErrors()).build();
            CERTOracle.CheckedFunction<SQLancerResultSet, Optional<Long>> rowCountParser = (rs) -> {
                int rowCount = rs.getInt(10);
                return Optional.of((long) rowCount);
            };
            CERTOracle.CheckedFunction<SQLancerResultSet, Optional<String>> queryPlanParser = (rs) -> {
                String operation = rs.getString(2);
                return Optional.of(operation);
            };

            return new CERTOracle<>(globalState, gen, expectedErrors, rowCountParser, queryPlanParser);

        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    },
    FUZZER {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws Exception {
            return new MySQLFuzzer(globalState);
        }

    },
    DQP {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
            return new MySQLDQPOracle(globalState);
        }
    },
    DQE {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
            return new MySQLDQEOracle(globalState);
        }
    },

    /**
     * Subset Oracle — Method 1.
     *
     * <p>Builds S1 (random schema + data) and S2 (S1's data plus extra rows),
     * then checks that aggregate functions (COUNT, MAX, MIN, EXISTS) satisfy
     * their expected monotonicity / anti-monotonicity properties.
     *
     * <p>Enable with: {@code --oracle SUBSET}
     */
    SUBSET {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws Exception {
            return new MySQLSubsetOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            // The oracle manages its own tables; the global schema need not be non-empty.
            return false;
        }
    },
    
    SUBSET2 {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws Exception {
            return new MySQLSubsetOracle2(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            // The oracle manages its own tables; the global schema need not be non-empty.
            return false;
        }
    },

    SUBSET3 {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws Exception {
            return new MySQLSubsetOracle3(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            // The oracle manages its own tables; the global schema need not be non-empty.
            return false;
        }
    };
}
