package cz.cuni.matfyz.collector.wrappers.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.*;
import cz.cuni.matfyz.collector.wrappers.exceptions.*;
import cz.cuni.matfyz.collector.wrappers.mongodb.components.MongoConnection;
import cz.cuni.matfyz.collector.wrappers.mongodb.components.MongoDataCollector;
import cz.cuni.matfyz.collector.wrappers.mongodb.components.MongoExplainPlanParser;
import cz.cuni.matfyz.collector.wrappers.mongodb.components.MongoQueryResultParser;
import cz.cuni.matfyz.collector.wrappers.mongodb.queryparser.MongoQueryParser;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import org.bson.Document;

/**
 * Class representing Wrapper for mongodb database
 */
public class MongoWrapper extends AbstractWrapper<Document, Document, Document> {

    private final MongoClient _client;
    private final MongoDatabase _database;
    private final MongoQueryParser _queryParser;


    public MongoWrapper(ConnectionData connectionData) {
        super(connectionData, new MongoExceptionsFactory(connectionData));
        _client = MongoClients.create(MongoResources.getConnectionLink(connectionData.host(), connectionData.port(), connectionData.user(), connectionData.password()));
        _database = _client.getDatabase(connectionData.databaseName());
        _queryParser = new MongoQueryParser(getExceptionsFactory(MongoExceptionsFactory.class));
    }

    @Override
    protected AbstractQueryResultParser<Document> createResultParser() {
        return new MongoQueryResultParser(getExceptionsFactory());
    }

    @Override
    protected AbstractExplainPlanParser<Document> createExplainPlanParser() {
        return new MongoExplainPlanParser(getExceptionsFactory());
    }

    @Override
    protected AbstractConnection<Document, Document, Document> createConnection(ExecutionContext<Document, Document, Document> context) throws ConnectionException {
        return new MongoConnection(_database, getExceptionsFactory());
    }

    @Override
    protected void setDependenciesBeforeExecutionIfNeeded(ExecutionContext<Document, Document, Document> context) throws WrapperException {
        ((MongoQueryResultParser)_resultParser).setConnection(context.getConnection(MongoConnection.class));
    }

    @Override
    protected Document parseInputQuery(ExecutionContext<Document, Document, Document> context) throws ParseException {
        return _queryParser.parseQueryToCommand(context.getInputQuery());
    }

    @Override
    protected AbstractDataCollector<Document, Document, Document> createDataCollector(ExecutionContext<Document, Document, Document> context) throws DataCollectException {
        try {
            return new MongoDataCollector(context, _resultParser);
        } catch (ConnectionException e) {
            throw getExceptionsFactory().dataCollectorNotInitialized(e);
        }
    }

    @Override
    protected void removeDependenciesAfterExecutionIfPossible(ExecutionContext<Document, Document, Document> context) {
        ((MongoQueryResultParser)_resultParser).removeConnection();
    }

    @Override
    public void close() {
        _client.close();
    }
}
