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
    public void saveExecution(String uuid, DataModel model) {
        try {
            Document document = new Document();
            document.put("id", uuid);
            document.put("model", model.toMap());
            _database.getCollection("executions").insertOne(document);
        } catch (MongoException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getExecutionResult(String uuid) {
        var result = _database.getCollection("executions").find(new Document("id", uuid));
        for (var document : result) {
            return document.get("model", Document.class).toJson();
        }
        return null;
    }

    @Override
    public boolean getExecutionStatus(String uuid) {
        try (var resultIter = _database.getCollection("executions").find(new Document("id", uuid)).iterator()) {
            return resultIter.hasNext();
        } catch (MongoException e) {
            e.printStackTrace();
            return false;
        }
    }
}
