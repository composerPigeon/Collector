package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.queryresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;

import java.sql.*;

/**
 * Class which represents the wrapper operating over PostgreSQL database
 */
public class PostgresWrapper extends AbstractWrapper {
    private final PostgresParser _parser;
    public PostgresWrapper(String host, int port, String datasetName, String user, String password) {
        super(host, port, datasetName, user, password);
        _parser =  new PostgresParser();
    }

    /**
     * Method which gets the main query executes it, parse explai tree and collect all statistical data about the result and then return as instance of DataModel
     * @param query inputted query
     * @return instance of DataModel
     * @throws WrapperException when some of the implementing exceptions occur during the process
     */
    @Override
    public DataModel executeQuery(String query) throws WrapperException {
        try (
           var connection = new PostgresConnection(PostgresResources.getConnectionLink(_hostName, _port, _datasetName, _userName, _password), _parser);
        ) {
            DataModel dataModel = new DataModel(query, PostgresResources.DATABASE_NAME, _datasetName);
            ConsumedResult result = connection.executeMainQuery(query, dataModel);

            var collector = new PostgresDataCollector(connection, dataModel, _datasetName);
            return collector.collectData(result);
        } catch (SQLException e) {
            throw new WrapperException(e);
        }
    }
}
