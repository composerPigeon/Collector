package cz.cuni.matfyz.collector.wrappers.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.mongodb.queryparser.MongoQueryParser;
import org.bson.Document;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;

import java.util.List;

public class MongoWrapper extends AbstractWrapper {

    public MongoClient _client;
    public MongoDatabase _database;
    public MongoParser _parser;
    public MongoWrapper(String host, int port, String datasetName, String user, String password) {
        super(host, port, datasetName, user, password);
        _client = MongoClients.create(MongoResources.getConnectionLink(host, port, user, password));
        _database = _client.getDatabase(datasetName);
        _parser = new MongoParser(_database);
    }

    @Override
    public DataModel executeQuery(String query) throws WrapperException {
        try (var connection = new MongoConnection(_database, _parser)) {
            var model = new DataModel(query, MongoResources.DATABASE_NAME, _datasetName);

            var command = MongoQueryParser.parseQueryToCommmand(query);
            var result = connection.executeMainQuery(command, model);

            var collector = new MongoDataCollector(connection, model, _datasetName);
            return collector.collectData(result);
        }
    }
}
