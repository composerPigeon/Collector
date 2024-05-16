package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.*;
import cz.cuni.matfyz.collector.wrappers.cachedresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;

import java.sql.*;
public class PostgresConnection extends AbstractConnection<String, ResultSet, String> {
    private final Connection _connection;
    private final Statement _statement;
    public PostgresConnection(String link, PostgresParser parser) throws SQLException {
        super(parser);
        _connection = DriverManager.getConnection(link);
        _connection.setReadOnly(true);
        _statement = _connection.createStatement();
    }
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

    @Override
    public CachedResult executeQuery(String query) throws QueryExecutionException {
        try (ResultSet rs = _statement.executeQuery(query)){
            return _parser.parseResult(rs);
        } catch (SQLException | ParseException e) {
            throw new QueryExecutionException(e);
        }
    }

    @Override
    public ConsumedResult executeQueryAndConsume(String query) throws QueryExecutionException {
        try (ResultSet rs = _statement.executeQuery(query)){
            return _parser.parseResultAndConsume(rs);
        } catch (SQLException | ParseException e) {
            throw new QueryExecutionException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        _statement.close();
        _connection.close();
    }
}
