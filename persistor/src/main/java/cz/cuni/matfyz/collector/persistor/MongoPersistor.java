package cz.cuni.matfyz.collector.persistor;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.model.DataModelException;
import org.bson.Document;

/**
 * Implementation of Abstract persistor using Mongo native driver
 */
public class MongoPersistor extends AbstractPersistor {

    public MongoClient _client;
    public MongoDatabase _database;

    public MongoPersistor(String hostName, int port, String datasetName, String userName, String password) {
        _client = MongoClients.create(_buildConnectionLink(hostName, port, userName, password));
        _database = _client.getDatabase(datasetName);
    }

    /**
     * Method for building connection link of inputted params
     * @param hostName hostName of database
     * @param port port of database
     * @param userName userName to authenticate
     * @param password password to authenticate
     * @return created link
     */
    private String _buildConnectionLink(String hostName, int port, String userName, String password) {
        if (userName.isEmpty() || password.isEmpty())
            return "mongodb://" + hostName + ':' + port;
        else
            return "mongodb://" + userName + ':' + password + '@' + hostName + ':' + port;
    }

    /**
     * Method for saving execution result to mongodb
     * @param uuid id of execution
     * @param model model of collected statistical data for this execution
     * @throws PersistorException when MongoException occur during process
     */
    @Override
    public void saveExecution(String uuid, DataModel model) throws PersistorException {
        try {
            Document document = new Document();
            document.put("_id", uuid);
            document.put("model", Document.parse(model.toJson()));
            _database.getCollection("executions").insertOne(document);
        } catch (MongoException | DataModelException e) {
            throw new PersistorException(e);
        }
    }

    /**
     *  Method for saving execution error if occured to mongodb
     * @param uuid id of execution
     * @param errMsg error message
     * @throws PersistorException when MongoException occur during process
     */
    @Override
    public void saveExecutionError(String uuid, String errMsg) throws PersistorException {
        try {
            Document document = new Document();
            document.put("_id", uuid);
            document.put("error", errMsg);
            _database.getCollection("executions").insertOne(document);
        } catch (MongoException e) {
            throw new PersistorException(e);
        }
    }

    /**
     * Method for getting execution result from mongodb
     * @param uuid id of execution
     * @return model or error message or null if execution do not exist
     * @throws PersistorException
     */
    @Override
    public String getExecutionResult(String uuid) throws PersistorException {
        try {
            var result = _database.getCollection("executions").find(new Document("_id", uuid));
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

    /**
     * Method for getting execution state
     * @param uuid id of execution
     * @return true if execution is present in mongodb and false otherwise
     * @throws PersistorException
     */
    @Override
    public boolean getExecutionStatus(String uuid) throws PersistorException {
        try (var resultIter = _database.getCollection("executions").find(new Document("_id", uuid)).iterator()) {
            return resultIter.hasNext();
        } catch (MongoException e) {
            throw new PersistorException(e);
        }
    }
}
