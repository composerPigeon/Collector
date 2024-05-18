package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.*;
import cz.cuni.matfyz.collector.wrappers.cachedresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;

import java.sql.*;

/**
 * Class representing connection to PostgreSQL database and enables to evaluate queries
 */
public class PostgresConnection extends AbstractConnection<String, ResultSet, String> {
    private final Connection _connection;
    private final Statement _statement;
    public PostgresConnection(String link, PostgresParser parser) throws SQLException {
        super(parser);
        _connection = DriverManager.getConnection(link);
        _connection.setReadOnly(true);
        _statement = _connection.createStatement();
    }

    /**
     * Method executing main query and parsing its result and explain tree
     * @param query inputted query
     * @param toModel DataModel which is used for storing data parsed from explain tree
     * @return instance of ConsumedResult
     * @throws QueryExecutionException when ParseException or SQLException occurs during the process
     */
    @Override
    public ConsumedResult executeMainQuery(String query, DataModel toModel) throws QueryExecutionException {
        try (ResultSet planResult = _statement.executeQuery(PostgresResources.getExplainPlanQuery(query))) {
            if (planResult.next()) {
                _parser.parseExplainTree(toModel, planResult.getString("QUERY PLAN"));
            }
            try (ResultSet result = _statement.executeQuery(query)) {
                return _parser.parseMainResult(result, toModel);
            }
        } catch (ParseException | SQLException e) {
            throw new QueryExecutionException(e);
        }
    }

    /**
     * Method executing ordinal query and caching all of its result to CachedResult instance
     * @param query inputted query
     * @return instance of CachedResult
     * @throws QueryExecutionException when SQLException or ParseException occur during the process
     */
    @Override
    public CachedResult executeQuery(String query) throws QueryExecutionException {
        try (ResultSet rs = _statement.executeQuery(query)){
            return _parser.parseResult(rs);
        } catch (SQLException | ParseException e) {
            throw new QueryExecutionException(e);
        }
    }

    /**
     * Method which executes inputted query and consume it
     * @param query inputted query
     * @return instance of ConsumedResult
     * @throws QueryExecutionException when SQLException of ParseException occur during the process
     */
    @Override
    public ConsumedResult executeQueryAndConsume(String query) throws QueryExecutionException {
        try (ResultSet rs = _statement.executeQuery(query)){
            return _parser.parseResultAndConsume(rs);
        } catch (SQLException | ParseException e) {
            throw new QueryExecutionException(e);
        }
    }

    /**
     * Method which implements the Interface AutoClosable and closes all database resources after query execution is finished
     * @throws SQLException from _statement.close() and _connection.close() if some problem occurs
     */
    @Override
    public void close() throws SQLException {
        _statement.close();
        _connection.close();
    }
}
