package cz.cuni.matfyz.collector.wrappers.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import cz.cuni.matfyz.collector.wrappers.mongodb.queryparser.MongoQueryParser;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;

/**
 * Class representing Wrapper for mongodb database
 */
public class MongoWrapper extends AbstractWrapper {

    protected MongoClient _client;
    protected MongoDatabase _database;
    protected MongoParser _parser;

    public MongoWrapper(String host, int port, String datasetName, String user, String password) {
        super(host, port, datasetName, user, password);
        _client = MongoClients.create(MongoResources.getConnectionLink(host, port, user, password));
        _database = _client.getDatabase(datasetName);
        _parser = new MongoParser(_database);
    }

    /**
     * Method which evaluates the main query, collects the data and return it as a model
     * @param query inputted query
     * @return instance of DataModel containing statistical measures of the inputted query
     * @throws WrapperException when some of its implementing exceptions occur
     */
    @Override
    public DataModel executeQuery(String query) throws WrapperException {
        try (var connection = new MongoConnection(_database, _parser)) {
            var model = DataModel.CreateForQuery(query, MongoResources.DATABASE_NAME, _datasetName);
            var command = MongoQueryParser.parseQueryToCommmand(query);
            var result = connection.executeMainQuery(command, model);

            var collector = new MongoDataCollector(connection, model, _datasetName);
            return collector.collectData(result);
        }
    }
}
