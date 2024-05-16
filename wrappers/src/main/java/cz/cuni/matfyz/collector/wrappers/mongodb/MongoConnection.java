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

public class MongoConnection extends AbstractConnection<Document, Document, Document> {

    MongoDatabase _database;
    public MongoConnection(MongoDatabase database, MongoParser parser) {
        super(parser);
        _database = database;
    }
    @Override
    public CachedResult executeQuery(Document query) throws QueryExecutionException {
        try {
            Document result = _database.runCommand(query);
            return _parser.parseResult(result);
        } catch (MongoException | ParseException e) {
            throw new QueryExecutionException(e);
        }
    }

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

    @Override
    public void close() {}
}
