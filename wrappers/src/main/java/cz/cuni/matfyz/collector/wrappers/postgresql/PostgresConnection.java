package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractConnection;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.QueryExecutionException;

import java.sql.*;

public class PostgresConnection extends AbstractConnection<String, ResultSet> {
    private final Connection _connection;
    private final Statement _mainStatement;
    private final Statement _statement;
    public PostgresConnection(String link, String user, String password) throws SQLException {
        _connection = DriverManager.getConnection(link);
        _mainStatement = _connection.createStatement();
        _statement = _connection.createStatement();
    }
    @Override
    public void executeMainQuery(String query) throws QueryExecutionException {
        try {
            ResultSet planResult = _mainStatement.executeQuery(PostgresResources.getExplainPlanQuery(query));
            if (planResult.next()) {
                _mainPlan = planResult.getString("QUERY PLAN");
            }
            _mainResult = _mainStatement.executeQuery(query);
            _lastQuery = query;
        } catch (SQLException e) {
            throw new QueryExecutionException(e);
        }

    }

    @Override
    public ResultSet executeQuery(String query) throws QueryExecutionException {
        try {
            return _statement.executeQuery(query);
        } catch (SQLException e) {
            throw new QueryExecutionException(e);
        }

    }

    @Override
    public void close() throws SQLException {
        _mainStatement.close();
        _statement.close();
        _connection.close();
    }
}
