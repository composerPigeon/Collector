package cz.cuni.matfyz.collector.wrappers.postgresql.components;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.AbstractConnection;
import cz.cuni.matfyz.collector.wrappers.exceptions.ConnectionException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperExceptionsFactory;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresResources;

import java.sql.*;

/**
 * Class representing connection to PostgreSQL database and enables to evaluate queries
 */
public class PostgresConnection extends AbstractConnection<ResultSet, String, String> {
    private final Connection _connection;
    private final Statement _statement;
    public PostgresConnection(String link, WrapperExceptionsFactory exceptionsFactory) throws ConnectionException {
        super(exceptionsFactory);
        try {
            _connection = DriverManager.getConnection(link);
            _connection.setReadOnly(true);
            _statement = _connection.createStatement();
        } catch (SQLException e) {
            throw exceptionsFactory.connectionNotInitialized(e);
        }
    }

    @Override
    public ResultWithPlan<ResultSet, String> executeWithExplain(String query) throws QueryExecutionException {
        try {
            ResultSet planResult = _statement.executeQuery(PostgresResources.getExplainPlanQuery(query));
            String plan = null;
            if (planResult.next()) {
                plan = planResult.getString("QUERY PLAN");
            }
            ResultSet result = _statement.executeQuery(query);
            return new ResultWithPlan<>(result, plan);
        } catch (SQLException e) {
            throw _exceptionsFactory.queryExecutionWithExplainFailed(e);
        }
    }

    /**
     * Method executing ordinal query and caching all of its result to CachedResult instance
     * @param query inputted query
     * @return instance of CachedResult
     * @throws QueryExecutionException when SQLException or ParseException occur during the process
     */
    @Override
    public ResultSet executeQuery(String query) throws QueryExecutionException {
        try {
            return _statement.executeQuery(query);
        } catch (SQLException e) {
            throw _exceptionsFactory.queryExecutionFailed(e);
        }
    }

    @Override
    public boolean isOpen() {
        try {
            return !_statement.isClosed();
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Method which implements the Interface AutoClosable and closes all database resources after query execution is finished
     * @throws QueryExecutionException from _statement.close() and _connection.close() if some problem occurs
     */
    @Override
    public void close() throws QueryExecutionException {
        try {
            _statement.close();
            _connection.close();
        }
        catch (SQLException e) {
            throw new QueryExecutionException(e);
        }
    }
}
