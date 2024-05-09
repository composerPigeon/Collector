package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;

import java.sql.*;

public class PostgresWrapper extends AbstractWrapper {
    private final PostgresParser _parser;
    public PostgresWrapper(String host, int port, String datasetName, String user, String password) {
        super(host, port, datasetName, user, password);
        _parser =  new PostgresParser();
    }

    @Override
    public DataModel executeQuery(String query) throws WrapperException {
        try (
           var connection = new PostgresConnection(PostgresResources.getConnectionLink(_hostName, _port, _datasetName, _userName, _password), _parser);
        ) {
            DataModel dataModel = new DataModel(query, PostgresResources.DATABASE_NAME, _datasetName);
            CachedResult result = connection.executeMainQuery(query, dataModel);

            var collector = new PostgresDataCollector(connection, dataModel, _datasetName);
            return collector.collectData(result);
        } catch (SQLException e) {
            throw new WrapperException(e);
        }
    }

    @Override
    public String toString() {
        return "Connection link: " + _hostName + '/' + _datasetName + "\n";
    }
}
