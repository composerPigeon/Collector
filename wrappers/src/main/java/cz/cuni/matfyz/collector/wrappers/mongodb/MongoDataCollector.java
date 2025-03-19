package cz.cuni.matfyz.collector.wrappers.mongodb;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractDataCollector;
import cz.cuni.matfyz.collector.wrappers.queryresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.queryresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import org.bson.Document;

import java.util.*;

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
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _collectPageSize() throws QueryExecutionException {
        _model.datasetData().setDataSetPageSize(MongoResources.DefaultSizes.PAGE_SIZE);
    }

    /**
     * Method which will save cache size of dataset
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _collectCacheDatasetSize() throws QueryExecutionException {
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
    private void _collectDatasetData() throws QueryExecutionException {
        CachedResult stats = _connection.executeQuery(MongoResources.getDatasetStatsCommand());

        if (stats.next()) {
            long size = stats.getLong("storageSize");
            _model.datasetData().setDataSetSize(size);
            long sizeInPages = (long) Math.ceil((double)size / _model.getPageSize());
            _model.datasetData().setDataSetSizeInPages(sizeInPages);
        }
        _collectCacheDatasetSize();
    }



    // Save column Data

    /**
     * Method which saves byte size for fields which are of object or string type
     * @param collectionName collection which is used for query
     * @param columnName used column name
     * @param columnType data type of column
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _collectStringObjectColumnByteSize(String collectionName, String columnName, String columnType) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(MongoResources.getAvgObjectStringSizeCommand(collectionName, columnName, columnType));
        if (result.next()) {
            int avgByteSize = (int)Math.round(result.getDouble("avg"));
            _model.datasetData().setColumnTypeByteSize(collectionName, columnName, columnType, avgByteSize);
        }
    }

    /**
     * Method which saves byte size for fields which are of number type
     * @param collectionName collection which is used for query
     * @param columnName used column name
     * @param columnType data type of column
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _collectNumberColumnByteSize(String collectionName, String columnName, String columnType) throws QueryExecutionException {
        Integer size = MongoResources.DefaultSizes.getAvgColumnSizeByType(columnType);
        if (size != null) {
            _model.datasetData().setColumnTypeByteSize(collectionName, columnName, columnType, size);
        }
    }

    /**
     * Method which saves average field byte size
     * @param collectionName collection which is used for query
     * @param columnName used column name
     * @param columnType data type of column
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _collectColumnByteSize(String collectionName, String columnName, String columnType) throws QueryExecutionException {
        if ("string".equals(columnType) || "object".equals(columnType) || "binData".equals(columnType)) {
            _collectStringObjectColumnByteSize(collectionName, columnName, columnType);
        } else
            _collectNumberColumnByteSize(collectionName, columnName, columnType);
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
    private void _collectColumnMandatory(String collectionName, String columnName) throws QueryExecutionException {
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
    private void _collectColumnType(String collectionName, String columnName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(MongoResources.getFieldTypeCommand(collectionName, columnName));
        List<Map.Entry<String, Integer>> types = new ArrayList<>();
        int maxCount = 0;

        while (result.next()) {
            int count = result.getInt("count");
            String type = result.getDocument("_id").getString("fieldType");
            types.add(Map.entry(type, count));
            maxCount += count;
        }

        for (var entry : types) {
            _collectColumnByteSize(collectionName, columnName, entry.getKey());
        }
    }

    /**
     * Metghod which collects all field data
     * @param collectionName collection used for query
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _collectColumnData(String collectionName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(MongoResources.getFieldsInCollectionCommand(collectionName));

        if (result.next()) {
            List<String> fieldNames = result.getList("allKeys", String.class);

            for (String fieldName : fieldNames) {
                _collectColumnType(collectionName, fieldName);
                _collectColumnMandatory(collectionName, fieldName);
            }
        }
    }

    // save Table data

    /**
     * Mathod used for saving all collection data
     * @param collectionName collection used in query
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _collectTableData(String collectionName) throws QueryExecutionException {
        CachedResult stats = _connection.executeQuery(MongoResources.getCollectionStatsCommand(collectionName));

        if (stats.next()) {
            long size = stats.getLong("storageSize");
            _model.datasetData().setTableByteSize(collectionName, size);
            long sizeInPages = (long) Math.ceil((double)size / _model.getPageSize());
            _model.datasetData().setTableSizeInPages(collectionName, sizeInPages);

            long rowCount = stats.getLong("count");
            _model.datasetData().setTableRowCount(collectionName, rowCount);
        }

        _collectColumnData(collectionName);
    }

    // Save Index Data

    /**
     * Method which saves record count for index
     * @param collectionName used collection in query
     * @param indexName used index
     * @throws QueryExecutionException when some QueryExecutionException occur during running help query
     */
    private void _collectIndexRowCount(String collectionName, String indexName) throws QueryExecutionException {
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
    private void _collectIndexSizesData(String collectionName, String indexName) throws QueryExecutionException {
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
    private void _collectIndexesData(String collectionName) throws QueryExecutionException {
        for (String indexName : _model.getIndexNames()) {
            _collectIndexSizesData(collectionName, indexName);
            _collectIndexRowCount(collectionName, indexName);
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
    private void _collectResultColumnData(ConsumedResult result) {
        for (String colName : result.getColumnNames()) {
            for (String colType : result.getColumnTypes(colName)) {
                if (colType != null) {
                    Integer size = MongoResources.DefaultSizes.getAvgColumnSizeByType(colType);
                    if (size != null)
                        _model.resultData().setColumnTypeByteSize(colName, colType, size);
                    double ratio = result.getColumnTypeRatio(colName, colType);
                    _model.resultData().setColumnTypeRatio(colName, colType, ratio);
                }
            }
        }
    }

    /**
     * Method which saves all result data from result
     * @param result result of main query
     */
    private void _collectResultData(ConsumedResult result) {
        long size = result.getByteSize();
        _model.resultData().setByteSize(size);
        long count = result.getRowCount();
        _model.resultData().setRowCount(count);

        long sizeInPages = (long)Math.ceil((double) size / _model.getPageSize());
        _model.resultData().setSizeInPages(sizeInPages);

        _collectResultColumnData(result);
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
            _collectPageSize();
            _collectDatasetData();
            _collectIndexesData(collName);
            _collectTableData(collName);
            _collectResultData(result);
            return _model;
        } catch (QueryExecutionException e) {
            throw new DataCollectException(e);
        }
    }
}
