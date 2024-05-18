package cz.cuni.matfyz.collector.wrappers.mongodb;

import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractConnection;

import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.cachedresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Mongodb implementation of AbstractConnection
 */
public class MongoConnection extends AbstractConnection<Document, Document, Document> {

    /**
     * Field containing reference to mongodb database over which is wrapper working
     */
    MongoDatabase _database;
    public MongoConnection(MongoDatabase database, MongoParser parser) {
        super(parser);
        _database = database;
    }

    /**
     * Implementation of abstract method that will execute query and cache whole result parsed to CachedResult
     * @param query inputted query
     * @return instance of CachedResult which corresponds to native result of inputted query
     * @throws QueryExecutionException when some MongoException or ParseException occur during process
     */
    @Override
    public CachedResult executeQuery(Document query) throws QueryExecutionException {
        try {
            Document result = _database.runCommand(query);
            return _parser.parseResult(result);
        } catch (MongoException | ParseException e) {
            throw new QueryExecutionException(e);
        }
    }

    /**
     * Method which executes main query and then consume its result and parse and get its explain plan
     * @param query inputted query
     * @param toModel DataModel which is used for storing data parsed from explain tree
     * @return instance of ConsumedResult which contains statistics of native result of inputted query
     * @throws QueryExecutionException when some MongoException or ParseException occur during process
     */
    @Override
    public ConsumedResult executeMainQuery(Document query, DataModel toModel) throws QueryExecutionException {
        try {
            Document result = _database.runCommand(query);
            Document explainTree = _database.runCommand(MongoResources.getExplainCommand(query));
            _parser.parseExplainTree(toModel, explainTree);
            return _parser.parseMainResult(result, toModel);
        } catch (MongoException | ParseException e) {
            throw new QueryExecutionException(e);
        }
    }

    /**
     * Method which executes main query and then consume its result
     * @param query inputted query
     * @return instance of ConsumedResult which contains statistics of native result of inputted query
     * @throws QueryExecutionException when some MongoException or ParseException occur during process
     */
    @Override
    public ConsumedResult executeQueryAndConsume(Document query) throws QueryExecutionException {
        try {
            Document result = _database.runCommand(query);
            return _parser.parseResultAndConsume(result);
        } catch (MongoException | ParseException e) {
            throw new QueryExecutionException(e);
        }
    }

    /**
     * Method which implements AutoClosable interface
     */
    @Override
    public void close() {}
}
