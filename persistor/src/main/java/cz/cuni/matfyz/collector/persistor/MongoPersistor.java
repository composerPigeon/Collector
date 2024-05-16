package cz.cuni.matfyz.collector.persistor;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import cz.cuni.matfyz.collector.model.DataModel;
import org.bson.Document;

public class MongoPersistor extends AbstractPersistor {

    public MongoClient _client;
    public MongoDatabase _database;

    public MongoPersistor(String hostName, int port, String datasetName, String userName, String password) {
        _client = MongoClients.create(_buildConnectionLink(hostName, port, userName, password));
        _database = _client.getDatabase(datasetName);
    }

    private String _buildConnectionLink(String hostName, int port, String userName, String password) {
        if (userName.isEmpty() || password.isEmpty())
            return "mongodb://" + hostName + ':' + port;
        else
            return "mongodb://" + userName + ':' + password + '@' + hostName + ':' + port;
    }

    @Override
    public void saveExecution(String uuid, DataModel model) throws PersistorException {
        try {
            Document document = new Document();
            document.put("id", uuid);
            document.put("model", model.toMap());
            _database.getCollection("executions").insertOne(document);
        } catch (MongoException e) {
            throw new PersistorException(e);
        }
    }

    @Override
    public void saveExecutionError(String uuid, String errMsg) throws PersistorException {
        try {
            Document document = new Document();
            document.put("id", uuid);
            document.put("error", errMsg);
            _database.getCollection("executions").insertOne(document);
        } catch (MongoException e) {
            throw new PersistorException(e);
        }
    }

    @Override
    public String getExecutionResult(String uuid) throws PersistorException {
        try {
            var result = _database.getCollection("executions").find(new Document("id", uuid));
            for (var document : result) {
                if (document.containsKey("model"))
                    return document.get("model", Document.class).toJson();
                else
                    return document.getString("error");
            }
            return null;
        } catch (MongoException e) {
            throw new PersistorException(e);
        }

    }

    @Override
    public boolean getExecutionStatus(String uuid) throws PersistorException {
        try (var resultIter = _database.getCollection("executions").find(new Document("id", uuid)).iterator()) {
            return resultIter.hasNext();
        } catch (MongoException e) {
            throw new PersistorException(e);
        }
    }
}
