package cz.cuni.matfyz.collector.wrappers.mongodb;

import com.mongodb.client.MongoDatabase;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractParser;
import cz.cuni.matfyz.collector.wrappers.queryresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.queryresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;

import java.util.List;
import java.util.Optional;

/**
 * Class responsible for parsing results and explain plans of Mongodb database
 */
public class MongoParser extends AbstractParser<Document, Document> {

    private final MongoDatabase _database;

    public MongoParser(MongoDatabase database) {
        _database = database;
    }

    /**
     * Method which parses collection name of query
     * @param model instance of DataModel where the name will be saved
     * @param command node of explain result
     */
    private void _parseTableNames(DataModel model, Document command) {
        //TODO: Add aggregate support, because of views
        if (command.containsKey("find")) {
            String collectionName = command.getString("find");
            if (collectionName != null)
                model.dataset().addTable(collectionName);
        }
        if (command.containsKey("aggregate")) {
            List<Document> pipeline = command.getList("pipeline", Document.class);
            for (Document stage : pipeline) {
                if (stage.containsKey("$lookup")) {
                    String collName = stage.get("$lookup", Document.class).getString("from");
                    model.dataset().addTable(collName);
                }
            }
        }
    }

    /**
     * Method which parses a stage of explain plan
     * @param model instance of DataModel where results are saved
     * @param stage actual stage to be parsed
     */
    private void _parseStage(DataModel model, Document stage) {
        if ("IXSCAN".equals(stage.getString("stage"))) {
            String indexName = stage.getString("indexName");
            if (indexName != null) {
                model.dataset().addIndex(indexName);
            }
        }
        if (stage.containsKey("inputStage")) {
            _parseStage(model, stage.get("inputStage", Document.class));
        }
    }

    /**
     * Method for parsing statistics about execution
     * @param model instance of DataModel where will all results be saved
     * @param node document from which the statistics will be parsed (especially the execution time)
     */
    private void _parseExecutionStats(DataModel model, Document node) {
        if (node.getBoolean("executionSuccess")) {

            model.result().setExecutionTime(Double.valueOf(node.getInteger("executionTimeMillis")));
        }
    }

    /**
     * Method for parsing epxlain plan and consume it into model
     * @param model instance of DataModel where collected information are stored
     * @param explainTree explain tree to be parsed
     * @throws ParseException is there to implements the abstract method
     */
    @Override
    public void parseExplainTree(DataModel model, Document explainTree) throws ParseException {
        _parseTableNames(model, explainTree.get("command", Document.class));
        _parseExecutionStats(model, explainTree.get("executionStats", Document.class));
        _parseStage(
                model,
                explainTree.get("queryPlanner", Document.class)
                        .get("winningPlan", Document.class)
                        .get("queryPlan", Document.class)
        );
    }

    // Parse Result

    /**
     * Mathod which parses BsonValue to type it is using
     * @param value the BsonValue to be parsed
     * @return string representation of parsed type
     */
    private String _parseType(BsonValue value) {
        if (value.isArray())
            return "array";
        else if (value.isBinary())
            return "binData";
        else if (value.isBoolean())
            return  "bool";
        else if (value.isDateTime())
            return "date";
        else if (value.isDecimal128())
            return "decimal";
        else if (value.isDocument())
            return "object";
        else if (value.isDouble())
            return "double";
        else if (value.isInt32())
            return "int";
        else if (value.isInt64())
            return "long";
        else if (value.isObjectId())
            return "objectId";
        else if (value.isString())
            return "string";
        else
            return null;
    }

    /**
     * Mathod which will fetch all documents from cursor to result
     * @param batch fetched documents
     * @param builder builder responsible for building the result
     */
    private void _addDocumentsToResult(List<Document> batch, CachedResult.Builder builder) {
        for (Document document : batch) {
            builder.addEmptyRecord();
            for (var pair : document.entrySet()) {
                builder.toLastRecordAddValue(pair.getKey(), pair.getValue());
            }
        }
    }

    /**
     * Method which will parse curor result to CachedResult
     * @param cursor cursor document from native result
     * @param builder for CachedResult used to build it
     */
    private void _parseCursorResult(Document cursor, CachedResult.Builder builder ) {
        if (cursor.containsKey("firstBatch")) {
            _addDocumentsToResult(cursor.getList("firstBatch", Document.class), builder);
        } else if (cursor.containsKey("nextBatch")) {
            _addDocumentsToResult(cursor.getList("nextBatch", Document.class), builder);
        }
    }

    /**
     * Method for parsing native result of ordinal query to instance of CachedResult
     * @param result result of some query
     * @return parsed CachedResult instance
     * @throws ParseException is there to implements the abstract method
     */
    @Override
    public CachedResult parseResult(Document result) throws ParseException {
        CachedResult.Builder builder = new CachedResult.Builder();

        if (result.containsKey("cursor"))
            _parseCursorResult(result.get("cursor", Document.class), builder);
        else {
            builder.addEmptyRecord();
            for (var pair : result.entrySet()) {
                builder.toLastRecordAddValue(pair.getKey(), pair.getValue());
            }
        }
        return builder.toResult();
    }


    // Parse Main Result

    /**
     * Method which will measure stats about document from result and add them to consumed result
     * @param document from native result
     * @param builder builder responsible for building the result
     */
    private void _parseColumnTypes(RawBsonDocument document, ConsumedResult.Builder builder) {
        for (var entry : document.entrySet()) {
            String fieldName = entry.getKey();
            if (!builder.containsTypeForCol(fieldName)) {
                String type = _parseType(entry.getValue());
                if (type != null)
                    builder.addColumnType(fieldName, type);
            }
        }
    }

    /**
     * Method consuming all documents from cursor to result
     * @param batch fetched documents
     * @param builder builder responsible for building the result
     */
    private void _consumeDocumentsToResult(List<Document> batch, ConsumedResult.Builder builder) {
        for (Document document : batch) {
            builder.addRecord();
            RawBsonDocument sizeDoc = RawBsonDocument.parse(document.toJson());
            builder.addByteSize(sizeDoc.getByteBuffer().remaining());
            _parseColumnTypes(sizeDoc, builder);
        }
    }

    /**
     * Method which is responsible to iterate through whole cursor
     * @param cursor cursor to iterate
     * @param builder builder to save results and build ConsumedResult
     * @param collectionName collection name used by main query
     */
    private void _parseMainCursorResult(Document cursor, ConsumedResult.Builder builder, String collectionName) {
        if (cursor.containsKey("firstBatch")) {
            _consumeDocumentsToResult(cursor.getList("firstBatch", Document.class), builder);
        } else if (cursor.containsKey("nextBatch")) {
            _consumeDocumentsToResult(cursor.getList("nextBatch", Document.class), builder);
        } else {
            return;
        }

        if (collectionName != null) {
            long cursorId = cursor.getLong("id");
            if (cursorId != 0) {
                Document result = _database.runCommand(MongoResources.getNextBatchOfCursorCommand(cursorId, collectionName));
                _parseMainCursorResult(result.get("cursor", Document.class), builder, collectionName);
            }
        }
    }

    /**
     * Method responsible for parsing native result of main query
     * @param result is native result of some query
     * @param withModel instance of DataModel for getting important data such as tableNames, so information about result columns can be gathered
     * @return instance of ConsumedResult
     * @throws ParseException is there to implements the abstract method
     */
    @Override
    public ConsumedResult parseMainResult(Document result, DataModel withModel) throws ParseException {
        ConsumedResult.Builder builder = new ConsumedResult.Builder();
        if (result.containsKey("cursor")) {
            Optional<String> optional = withModel.getTableNames().stream().findFirst();
            String collectionName = optional.orElse(null);

            _parseMainCursorResult(result.get("cursor", Document.class), builder, collectionName);
        }
        else {
            builder.addRecord();
            RawBsonDocument sizeDoc = RawBsonDocument.parse(result.toJson());
            builder.addByteSize(sizeDoc.getByteBuffer().remaining());
            _parseColumnTypes(sizeDoc, builder);
        }
        return builder.toResult();
    }

    /**
     * Method responsible for consuming result into ConsumedResult
     * @param result is native result of some query
     * @return instance of ConsumedResult
     * @throws ParseException is there to implement abstract method
     */
    @Override
    public ConsumedResult parseResultAndConsume(Document result) throws ParseException {
        ConsumedResult.Builder builder = new ConsumedResult.Builder();
        if (result.containsKey("cursor")) {
            _parseMainCursorResult(result.get("cursor", Document.class), builder, null);
        }
        else {
            builder.addRecord();
            RawBsonDocument sizeDoc = RawBsonDocument.parse(result.toJson());
            builder.addByteSize(sizeDoc.getByteBuffer().remaining());
            _parseColumnTypes(sizeDoc, builder);
        }
        return builder.toResult();
    }
}
