package cz.cuni.matfyz.collector.wrappers.mongodb;

import com.mongodb.client.MongoDatabase;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractDataCollector;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.cachedresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import org.bson.Document;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MongoDataCollector extends AbstractDataCollector<Document, Document, Document> {

    public MongoDataCollector(MongoConnection connection, DataModel model, String datasetName) {
        super(datasetName, model, connection);
    }

    // Save Dataset data
    private void _savePageSize(String collectionName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(MongoResources.getCollectionStatsCommand(collectionName));

        if (result.next()) {
            int pageSize = result.getDocument("wiredTiger").get("block-manager", Document.class).getInteger("file allocation unit size");
            _model.datasetData().setDataSetPageSize(pageSize);
        }
    }

    private void _saveDatasetData() throws QueryExecutionException {
        CachedResult stats = _connection.executeQuery(MongoResources.getDatasetStatsCommand());

        if (stats.next()) {
            long size = stats.getLong("storageSize");
            _model.datasetData().setDataSetSize(size);
            long sizeInPages = (long) Math.ceil((double)size / _model.getPageSize());
            _model.datasetData().setDataSetSizeInPages(sizeInPages);
        }
    }



    // Save column Data
    private void _saveStringObjectColumnByteSize(String collectionName, String columnName, String columnType) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(MongoResources.getAvgObjectStringSizeCommand(collectionName, columnName, columnType));
        if (result.next()) {
            int avgByteSize = (int)Math.round(result.getDouble("avg"));
            _model.datasetData().setColumnByteSize(collectionName, columnName, avgByteSize);
        }
    }

    private void _saveNumberColumnByteSize(String collectioName, String columnName, String columnType) throws QueryExecutionException {
        Integer size = MongoResources.DefaultSizes.getAvgColumnSizeByType(columnType);
        if (size != null) {
            _model.datasetData().setColumnByteSize(collectioName, columnName, size);
        }
    }
    private void _saveColumnByteSize(String collectionName, String columnName, String columnType) throws QueryExecutionException {
        if ("string".equals(columnType) || "object".equals(columnType) || "binData".equals(columnType)) {
            _saveStringObjectColumnByteSize(collectionName, columnName, columnType);
        } else
            _saveNumberColumnByteSize(collectionName, columnName, columnType);
    }

    private boolean _isRequiredField(Document options, String columnName) {
        if ("_id".equals(columnName))
            return true;

        if (options.containsKey("validator")) {
            Document validator = options.get("validator", Document.class);
            if (validator.containsKey("$jsonSchema")) {
                Document schema = validator.get("$jsonSchema", Document.class);
                if (schema.containsKey("required")) {
                    List<String> fields = schema.getList("required", String.class);
                    return fields.contains(columnName);
                }
            }
        }
        return false;
    }

    private void _saveColumnMandatory(String collectionName, String columnName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(MongoResources.getCollectionInfoCommand(collectionName));
        if (result.next()) {
            if (result.containsCol("options")) {
                boolean isRequired = _isRequiredField(result.getDocument("options"), columnName);
                _model.datasetData().setColumnMandatory(collectionName, columnName, isRequired);
            } else {
                boolean isRequired = "_id".equals(columnName);
                _model.datasetData().setColumnMandatory(collectionName, columnName, isRequired);
            }
        }
    }

    private void _saveColumnType(String collectionName, String columnName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(MongoResources.getFieldTypeCommand(collectionName, columnName));

        int bestCount = 0;
        String type = "";

        while (result.next()) {
            int count = result.getInt("count");

            if (count > bestCount) {
                bestCount = count;
                type = result.getDocument("_id").getString("fieldType");
            }
        }

        if (bestCount > 0) {
            _model.datasetData().setColumnType(collectionName, columnName, type);
            _saveColumnByteSize(collectionName, columnName, type);
        }
    }

    private void _saveColumnData(String collectionName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(MongoResources.getFieldsInCollectionCommand(collectionName));

        if (result.next()) {
            List<String> fieldNames = result.getList("allKeys", String.class);

            for (String fieldName : fieldNames) {
                _saveColumnType(collectionName, fieldName);
                _saveColumnMandatory(collectionName, fieldName);
            }
        }
    }

    // save Table data
    private void _saveTableData(String collectionName) throws QueryExecutionException {
        CachedResult stats = _connection.executeQuery(MongoResources.getCollectionStatsCommand(collectionName));

        if (stats.next()) {
            long size = stats.getLong("storageSize");
            _model.datasetData().setTableByteSize(collectionName, size);
            long sizeInPages = (long) Math.ceil((double)size / _model.getPageSize());
            _model.datasetData().setTableSizeInPages(collectionName, sizeInPages);

            long rowCount = stats.getLong("count");
            _model.datasetData().setTableRowCount(collectionName, rowCount);
        }

        _saveColumnData(collectionName);
    }

    // Save Index Data
    private void _saveIndexRowCount(String collectionName, String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(MongoResources.getIndexRowCountCommand(collectionName, indexName));

        if (result.next()) {
            long count = result.getLong("n");
            _model.datasetData().setIndexRowCount(indexName, count);
        }
    }

    private void _saveIndexSizesData(String collectionName, String indexName) throws QueryExecutionException {
        CachedResult stats = _connection.executeQuery(MongoResources.getCollectionStatsCommand(collectionName));
        if (stats.next()) {
            int size = stats.getDocument("indexSizes").getInteger(indexName);
            _model.datasetData().setIndexByteSize(indexName, size);
            _model.datasetData().setIndexSizeInPages(indexName, (long)Math.ceil((double)size / _model.getPageSize()));
        }
    }

    private void _saveIndexesData(String collectionName) throws QueryExecutionException {
        for (String indexName : _model.getIndexNames()) {
            _saveIndexSizesData(collectionName, indexName);
            _saveIndexRowCount(collectionName, indexName);
        }
    }

    private String _getCollectionName() throws DataCollectException {
        for (String collectionName : _model.getTableNames()) {
            return collectionName;
        }
        throw new DataCollectException("No mongodb collection parsed from query");
    }

    //Save Result data
    private void _saveResultColumnData(ConsumedResult result) {
        for (String colName : result.getColumnNames()) {
            String type = result.getColumnType(colName);
            if (type != null) {
                _model.resultData().setColumnType(colName, type);
                Integer size = MongoResources.DefaultSizes.getAvgColumnSizeByType(type);
                if (size != null)
                    _model.resultData().setColumnByteSize(colName, size);
            }
        }
    }

    private void _saveResultData(ConsumedResult result) {
        long size = result.getByteSize();
        _model.resultData().setByteSize(size);
        long count = result.getRowCount();
        _model.resultData().setRowCount(count);

        long sizeInPages = (long)Math.ceil((double) size / _model.getPageSize());
        _model.resultData().setSizeInPages(sizeInPages);

        _saveResultColumnData(result);
    }

    @Override
    public DataModel collectData(ConsumedResult result) throws DataCollectException {
        try {
            String collName = _getCollectionName();
            _savePageSize(collName);
            _saveDatasetData();
            _saveIndexesData(collName);
            _saveTableData(collName);
            _saveResultData(result);
            return _model;
        } catch (QueryExecutionException e) {
            throw new DataCollectException(e);
        }
    }
}
