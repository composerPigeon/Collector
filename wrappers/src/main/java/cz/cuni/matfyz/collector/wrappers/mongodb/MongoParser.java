package cz.cuni.matfyz.collector.wrappers.mongodb;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractParser;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import org.bson.Document;

public class MongoParser extends AbstractParser<Document, Document> {

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

    @Override
    public CachedResult parseResult(Document result) throws ParseException {
        CachedResult.Builder builder = new CachedResult.Builder();
        System.out.println(result.toJson());
        builder.addEmptyRecord();
        for (var pair : result.entrySet()) {
            builder.toLastRecordAddValue(pair.getKey(), pair.getValue());
        }
        return builder.toResult();
    }

    @Override
    public CachedResult parseMainResult(Document result, DataModel withModel) throws ParseException {
        CachedResult.Builder builder = new CachedResult.Builder();
        for (Document document : result.get("cursor", Document.class).getList("firstBatch", Document.class)) {
            builder.addEmptyRecord();
            for (var pair : document.entrySet()) {
                builder.toLastRecordAddValue(pair.getKey(), pair.getValue());
            }
        }
        return builder.toResult();
    }
}
