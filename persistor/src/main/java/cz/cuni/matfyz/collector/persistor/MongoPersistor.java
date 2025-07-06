package cz.cuni.matfyz.collector.persistor;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.model.DataModelException;
import org.bson.Document;

import java.util.Map;

/**
 * Implementation of Abstract persistor using Mongo native driver
 */
public class MongoPersistor extends AbstractPersistor {
    private final MongoClient _client;
    private final MongoDatabase _database;

    public MongoPersistor(AbstractPersistor.ConnectionData connectionData) {
        super(connectionData);
        _client = MongoClients.create(_buildConnectionLink(
                connectionData.host(),
                connectionData.port(),
                connectionData.user(),
                connectionData.password()
        ));
        _database = _client.getDatabase(connectionData.databaseName());
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
    public void saveExecutionResult(String uuid, DataModel model) throws PersistorException {
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
     *  Method for saving execution error if occurred to mongodb
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
     */
    @Override
    public ExecutionResult getExecutionResult(String uuid) throws PersistorException {
        try {
            var result = _database.getCollection("executions").find(new Document("_id", uuid));
            for (var document : result) {
                if (document.containsKey("model")) {
                    Map<String, Object> value = document.get("model", Document.class);
                    return ExecutionResult.success(value);
                } else {
                    var errorMessage = document.getString("error");
                    return ExecutionResult.error(errorMessage);
                }
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
     * @throws PersistorException when problem occurs
     */
    @Override
    public boolean containsExecution(String uuid) throws PersistorException {
        try (var result = _database.getCollection("executions").find(new Document("_id", uuid)).iterator()) {
            return result.hasNext();
        } catch (MongoException e) {
            throw new PersistorException(e);
        }
    }

    @Override
    public void close() {
        _client.close();
    }
}
