package sqlancer;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLConnection implements SQLancerDBConnection {

    private static final int DEFAULT_STATEMENT_TIMEOUT_SECONDS =
            Integer.getInteger("sqlancer.jdbc.queryTimeoutSeconds", 600);

    private final Connection connection;

    public SQLConnection(Connection connection) {
        this.connection = connection;
    }

    @Override
    public String getDatabaseVersion() throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        return meta.getDatabaseProductVersion();
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    public Statement prepareStatement(String arg) throws SQLException {
        Statement s = connection.prepareStatement(arg);
        applyStatementTimeout(s);
        return s;
    }

    public Statement createStatement() throws SQLException {
        Statement s = connection.createStatement();
        applyStatementTimeout(s);
        return s;
    }

    private void applyStatementTimeout(Statement s) throws SQLException {
        if (DEFAULT_STATEMENT_TIMEOUT_SECONDS > 0) {
            s.setQueryTimeout(DEFAULT_STATEMENT_TIMEOUT_SECONDS);
        }
    }
}
