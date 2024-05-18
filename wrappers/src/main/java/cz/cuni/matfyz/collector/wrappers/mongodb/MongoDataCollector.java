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

/**
 * Class which is responsible for collecting all the statistical data for mongodb wrapper after query is evaluated
 */
public class MongoDataCollector extends AbstractDataCollector<Document, Document, Document> {

    public MongoDataCollector(MongoConnection connection, DataModel model, String datasetName) {
        super(datasetName, model, connection);
    }

    // Save Dataset data

    /**
     * Method which will save page size
     * @param collectionName inputted collectionName
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _savePageSize(String collectionName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(MongoResources.getCollectionStatsCommand(collectionName));

        if (result.next()) {
            int pageSize = result.getDocument("wiredTiger").get("block-manager", Document.class).getInteger("file allocation unit size");
            _model.datasetData().setDataSetPageSize(pageSize);
        }
    }

    /**
     * Method which will save cache size of dataset
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _saveCacheDatasetSize() throws QueryExecutionException {
        CachedResult stats = _connection.executeQuery(MongoResources.getServerStatsCommand());

        if (stats.next()) {
            long size = stats.getDocument("wiredTiger").get("cache", Document.class).getLong("maximum bytes configured");
            _model.datasetData().setDataSetCacheSize(size);
        }
    }

    /**
     * Method which will save all dataset data wrapper gathers
     * @throws QueryExecutionException when some QueryExecutionException occur during running help queries
     */
    private void _saveDatasetData() throws QueryExecutionException {
        CachedResult stats = _connection.executeQuery(MongoResources.getDatasetStatsCommand());

        if (stats.next()) {
            long size = stats.getLong("storageSize");
            _model.datasetData().setDataSetSize(size);
            long sizeInPages = (long) Math.ceil((double)size / _model.getPageSize());
            _model.datasetData().setDataSetSizeInPages(sizeInPages);
        }
        _saveCacheDatasetSize();
    }



    // Save column Data

    /**
     * Method which saves byte size for fields which are of object or string type
     * @param collectionName collection which is used for query
     * @param columnName used column name
     * @param columnType data type of column
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _saveStringObjectColumnByteSize(String collectionName, String columnName, String columnType) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(MongoResources.getAvgObjectStringSizeCommand(collectionName, columnName, columnType));
        if (result.next()) {
            int avgByteSize = (int)Math.round(result.getDouble("avg"));
            _model.datasetData().setColumnByteSize(collectionName, columnName, avgByteSize);
        }
    }

    /**
     * Method which saves byte size for fields which are of number type
     * @param collectionName collection which is used for query
     * @param columnName used column name
     * @param columnType data type of column
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _saveNumberColumnByteSize(String collectionName, String columnName, String columnType) throws QueryExecutionException {
        Integer size = MongoResources.DefaultSizes.getAvgColumnSizeByType(columnType);
        if (size != null) {
            _model.datasetData().setColumnByteSize(collectionName, columnName, size);
        }
    }

    /**
     * Method which saves average field byte size
     * @param collectionName collection which is used for query
     * @param columnName used column name
     * @param columnType data type of column
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _saveColumnByteSize(String collectionName, String columnName, String columnType) throws QueryExecutionException {
        if ("string".equals(columnType) || "object".equals(columnType) || "binData".equals(columnType)) {
            _saveStringObjectColumnByteSize(collectionName, columnName, columnType);
        } else
            _saveNumberColumnByteSize(collectionName, columnName, columnType);
    }

    /**
     * Method which checks if field is required inside collection or no
     * @param options part of query result from which we analyze the fact
     * @param columnName which field we are interested
     * @return true if field is required
     */

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

    /**
     * Method which saves fact if field is mandatory
     * @param collectionName collection which is used for query
     * @param columnName used column name
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
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

    /**
     * Method which saves fields data type
     * @param collectionName collection which is used for query
     * @param columnName used column name
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
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

    /**
     * Metghod which collects all field data
     * @param collectionName collection used for query
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
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

    /**
     * Mathod used for saving all collection data
     * @param collectionName collection used in query
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
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

    /**
     * Method which saves record count for index
     * @param collectionName used collection in query
     * @param indexName used index
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _saveIndexRowCount(String collectionName, String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(MongoResources.getIndexRowCountCommand(collectionName, indexName));

        if (result.next()) {
            long count = result.getLong("n");
            _model.datasetData().setIndexRowCount(indexName, count);
        }
    }

    /**
     * Method which collects index sizes for specified index
     * @param collectionName used collection
     * @param indexName used index
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _saveIndexSizesData(String collectionName, String indexName) throws QueryExecutionException {
        CachedResult stats = _connection.executeQuery(MongoResources.getCollectionStatsCommand(collectionName));
        if (stats.next()) {
            int size = stats.getDocument("indexSizes").getInteger(indexName);
            _model.datasetData().setIndexByteSize(indexName, size);
            _model.datasetData().setIndexSizeInPages(indexName, (long)Math.ceil((double)size / _model.getPageSize()));
        }
    }

    /**
     * Method which collects all index data about all used indexes
     * @param collectionName used collection
     * @throws QueryExecutionException when some QueryExecutionException occur during running help queries
     */
    private void _saveIndexesData(String collectionName) throws QueryExecutionException {
        for (String indexName : _model.getIndexNames()) {
            _saveIndexSizesData(collectionName, indexName);
            _saveIndexRowCount(collectionName, indexName);
        }
    }

    /**
     * Get collection used by query
     * @return collection name
     * @throws DataCollectException when no such collection was parsed from query
     */
    private String _getCollectionName() throws DataCollectException {
        for (String collectionName : _model.getTableNames()) {
            return collectionName;
        }
        throw new DataCollectException("No mongodb collection parsed from query");
    }

    //Save Result data

    /**
     * Method which will save field data for field present in result
     * @param result result of executed main query
     */
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

    /**
     * Method which saves all result data from result
     * @param result result of main query
     */
    private void _saveResultData(ConsumedResult result) {
        long size = result.getByteSize();
        _model.resultData().setByteSize(size);
        long count = result.getRowCount();
        _model.resultData().setRowCount(count);

        long sizeInPages = (long)Math.ceil((double) size / _model.getPageSize());
        _model.resultData().setSizeInPages(sizeInPages);

        _saveResultColumnData(result);
    }

    /**
     *  Public method which collects all the statistical data for result
     * @param result result of main query
     * @return instance of DataModel containing all measured values
     */
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
