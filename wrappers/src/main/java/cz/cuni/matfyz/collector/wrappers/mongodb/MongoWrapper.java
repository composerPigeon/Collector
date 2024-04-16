package cz.cuni.matfyz.collector.wrappers.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;

import java.util.List;

public class MongoWrapper extends AbstractWrapper<Document, List<Document>> {

    public MongoClient _client;
    public MongoDatabase _database;
    public MongoWrapper(String link, String datasetName) {
        super(link, datasetName);
        _client = MongoClients.create(link);
        _database = _client.getDatabase(datasetName);
    }

    @Override
    public DataModel executeQuery(String query) throws WrapperException {
        Document command = Document.parse(query);
        _database.runCommand(command);

        return null;
    }
}
