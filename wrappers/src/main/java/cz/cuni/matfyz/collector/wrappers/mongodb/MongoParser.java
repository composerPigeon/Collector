package cz.cuni.matfyz.collector.wrappers.mongodb;

import com.mongodb.client.MongoDatabase;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractParser;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.cachedresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonArray;
import org.bson.RawBsonDocument;

import javax.print.Doc;
import java.util.List;
import java.util.Optional;
import java.util.RandomAccess;

public class MongoParser extends AbstractParser<Document, Document> {

    private final MongoDatabase _database;

    public MongoParser(MongoDatabase database) {
        _database = database;
    }

    private void _parseTableNames(DataModel model, Document command) {
        String collectionName = command.getString("find");
        if (collectionName != null)
            model.datasetData().addTable(collectionName);
    }

    private void _parseStage(DataModel model, Document stage) {
        if ("IXSCAN".equals(stage.getString("stage"))) {
            String indexName = stage.getString("indexName");
            if (indexName != null) {
                model.datasetData().addIndex(indexName);
            }
        }
        if (stage.containsKey("inputStage")) {
            _parseStage(model, stage.get("inputStage", Document.class));
        }
    }

    private void _parseExecutionStats(DataModel model, Document node) {
        if (node.getBoolean("executionSuccess")) {

            model.resultData().setExecutionTime(Double.valueOf(node.getInteger("executionTimeMillis")));
        }
    }
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
        else
            return null;
    }

    // parse Result
    private void _addDocumentsToResult(List<Document> batch, CachedResult.Builder builder) {
        for (Document document : batch) {
            builder.addEmptyRecord();
            for (var pair : document.entrySet()) {
                builder.toLastRecordAddValue(pair.getKey(), pair.getValue());
            }
        }
    }

    private void _parseCursorResult(Document cursor, CachedResult.Builder builder ) {
        if (cursor.containsKey("firstBatch")) {
            _addDocumentsToResult(cursor.getList("firstBatch", Document.class), builder);
        } else if (cursor.containsKey("nextBatch")) {
            _addDocumentsToResult(cursor.getList("nextBatch", Document.class), builder);
        }
    }

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

    private void _consumeDocumentsToResult(List<Document> batch, ConsumedResult.Builder builder) {
        for (Document document : batch) {
            builder.addRecord();
            RawBsonDocument sizeDoc = RawBsonDocument.parse(document.toJson());
            builder.addByteSize(sizeDoc.getByteBuffer().remaining());
            _parseColumnTypes(sizeDoc, builder);
        }
    }

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
