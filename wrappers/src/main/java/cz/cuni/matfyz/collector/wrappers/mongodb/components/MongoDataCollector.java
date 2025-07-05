package cz.cuni.matfyz.collector.wrappers.mongodb.components;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.AbstractDataCollector;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.AbstractQueryResultParser;
import cz.cuni.matfyz.collector.wrappers.exceptions.ConnectionException;
import cz.cuni.matfyz.collector.wrappers.mongodb.MongoExceptionsFactory;
import cz.cuni.matfyz.collector.wrappers.mongodb.MongoResources;
import cz.cuni.matfyz.collector.wrappers.queryresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.queryresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import org.bson.Document;

import java.util.*;

/**
 * Class which is responsible for collecting all the statistical data for mongodb wrapper after query is evaluated
 */
public class MongoDataCollector extends AbstractDataCollector<Document, Document, Document> {

    public MongoDataCollector(
            AbstractWrapper.ExecutionContext<Document, Document, Document> context,
            AbstractQueryResultParser<Document> resultParser
    ) throws ConnectionException {
        super(context, resultParser);
    }

    // Save Dataset data

    /**
     * Method which will save page size
     */
    private void _collectPageSize() {
        getModel().setPageSize(MongoResources.DefaultSizes.PAGE_SIZE);
    }

    /**
     * Method which will save cache size of dataset
     * @throws DataCollectException when some QueryExecutionException occur during running help query
     */
    private void _collectCacheDatabaseSize() throws DataCollectException {
        CachedResult stats = executeQuery(MongoResources.getServerStatsCommand());

        if (stats.next()) {
            long size = stats.getDocument("wiredTiger").get("cache", Document.class).getLong("maximum bytes configured");
            getModel().setDatabaseCacheSize(size);
        }
    }

    /**
     * Method which will save all dataset data wrapper gathers
     * @throws DataCollectException when some QueryExecutionException occur during running help queries
     */
    private void _collectDatabaseData() throws DataCollectException {
        CachedResult stats = executeQuery(MongoResources.getDatasetStatsCommand());

        if (stats.next()) {
            long size = stats.getLong("totalSize");

            getModel().setDatabaseByteSize(size);
            long sizeInPages = (long) Math.ceil((double)size / getModel().getPageSize());
            getModel().setDatabaseSizeInPages(sizeInPages);
        }
        _collectCacheDatabaseSize();
    }



    // Save column Data

    /**
     * Method which saves byte size for fields which are of object or string type
     * @param collectionName collection which is used for query
     * @param columnName used column name
     * @param columnType data type of column
     * @throws DataCollectException when some QueryExecutionException occur during running help query
     */
    private void _collectStringObjectFieldByteSize(String collectionName, String columnName, String columnType) throws DataCollectException {
        CachedResult result = executeQuery(MongoResources.getAvgObjectStringSizeCommand(collectionName, columnName, columnType));
        if (result.next()) {
            int avgByteSize = (int)Math.round(result.getDouble("avg"));
            getModel().setAttributeTypeByteSize(collectionName, columnName, columnType, avgByteSize);
        }
    }

    /**
     * Method which saves byte size for fields which are of number type
     * @param collectionName collection which is used for query
     * @param columnName used column name
     * @param columnType data type of column
     */
    private void _collectNumberFieldByteSize(String collectionName, String columnName, String columnType) {
        Integer size = MongoResources.DefaultSizes.getAvgColumnSizeByType(columnType);
        if (size != null) {
            getModel().setAttributeTypeByteSize(collectionName, columnName, columnType, size);
        }
    }

    /**
     * Method which saves average field byte size
     * @param collectionName collection which is used for query
     * @param fieldName used column name
     * @param fieldType data type of column
     * @throws DataCollectException when some QueryExecutionException occur during running help query
     */
    private void _collectFieldByteSize(String collectionName, String fieldName, String fieldType) throws DataCollectException {
        if ("string".equals(fieldType) || "object".equals(fieldType) || "binData".equals(fieldType)) {
            _collectStringObjectFieldByteSize(collectionName, fieldName, fieldType);
        } else
            _collectNumberFieldByteSize(collectionName, fieldName, fieldType);
    }

    /**
     * Method which checks if field is required inside collection or no
     * @param options part of query result from which we analyze the fact
     * @param fieldName which field we are interested
     * @return true if field is required
     */

    private boolean _isRequiredField(Document options, String fieldName) {
        if ("_id".equals(fieldName))
            return true;

        if (options.containsKey("validator")) {
            Document validator = options.get("validator", Document.class);
            if (validator.containsKey("$jsonSchema")) {
                Document schema = validator.get("$jsonSchema", Document.class);
                if (schema.containsKey("required")) {
                    List<String> fields = schema.getList("required", String.class);
                    return fields.contains(fieldName);
                }
            }
        }
        return false;
    }

    private void _collectFieldDistinctValuesCount(String collectionName, String fieldName) throws DataCollectException {
        CachedResult result = executeQuery(MongoResources.getFieldDistinctValuesCountQuery(collectionName, fieldName));

        if (result.next()) {
            long count = result.getLong("count");
            getModel().setAttributeDistinctValuesCount(collectionName, fieldName, count);
        }
    }

    /**
     * Method which saves fact if field is mandatory
     * @param collectionName collection which is used for query
     * @param fieldName used column name
     * @throws DataCollectException when some QueryExecutionException occur during running help query
     */
    private void _collectFieldMandatory(String collectionName, String fieldName) throws DataCollectException {
        CachedResult result = executeQuery(MongoResources.getCollectionInfoCommand(collectionName));
        if (result.next()) {
            if (result.containsAttribute("options")) {
                boolean isRequired = _isRequiredField(result.getDocument("options"), fieldName);
                getModel().setAttributeMandatory(collectionName, fieldName, isRequired);
            } else {
                boolean isRequired = "_id".equals(fieldName);
                getModel().setAttributeMandatory(collectionName, fieldName, isRequired);
            }
        }
    }

    /**
     * Method which saves fields data type
     * @param collectionName collection which is used for query
     * @param fieldName used column name
     * @throws DataCollectException when some QueryExecutionException occur during running help query
     */
    private void _collectFieldType(String collectionName, String fieldName) throws DataCollectException {
        CachedResult result = executeQuery(MongoResources.getFieldTypeCommand(collectionName, fieldName));
        List<Map.Entry<String, Integer>> types = new ArrayList<>();
        int maxCount = 0;

        while (result.next()) {
            int count = result.getInt("count");
            String type = result.getDocument("_id").getString("fieldType");
            types.add(Map.entry(type, count));
            maxCount += count;
        }

        for (var entry : types) {
            _collectFieldByteSize(collectionName, fieldName, entry.getKey());
        }

        if (maxCount > 0) {
            for (var entry : types) {
                getModel().setAttributeTypeRatio(
                        collectionName,
                        fieldName,
                        entry.getKey(),
                        (double)entry.getValue() / maxCount);
            }
        }
    }

    /**
     * Collects all field data
     * @param collectionName collection used for query
     * @throws DataCollectException when some QueryExecutionException occur during running help query
     */
    private void _collectFieldData(String collectionName) throws DataCollectException {
        CachedResult result = executeQuery(MongoResources.getFieldsInCollectionCommand(collectionName));

        if (result.next()) {
            List<String> fieldNames = result.getList("allKeys", String.class);

            for (String fieldName : fieldNames) {
                _collectFieldType(collectionName, fieldName);
                _collectFieldMandatory(collectionName, fieldName);
                _collectFieldDistinctValuesCount(collectionName, fieldName);
            }
        }
    }

    // save Table data

    /**
     * Method used for saving all collection data
     * @param collectionName collection used in query
     * @throws DataCollectException some QueryExecutionException occur during running help query
     */
    private void _collectCollectionData(String collectionName) throws DataCollectException {
        CachedResult stats = executeQuery(MongoResources.getCollectionStatsCommand(collectionName));

        if (stats.next()) {
            long size = stats.getLong("storageSize");
            getModel().setKindByteSize(collectionName, size);
            long sizeInPages = (long) Math.ceil((double)size / getModel().getPageSize());
            getModel().setKindSizeInPages(collectionName, sizeInPages);

            long recordCount = stats.getLong("count");
            getModel().setKindRecordCount(collectionName, recordCount);
        }

        _collectFieldData(collectionName);
    }

    // Save Index Data

    /**
     * Method which saves record count for index
     * @param collectionName used collection in query
     * @param indexName used index
     * @throws DataCollectException when some QueryExecutionException occur during running help query
     */
    private void _collectIndexRecordCount(String collectionName, String indexName) throws DataCollectException {
        CachedResult result = executeQuery(MongoResources.getIndexRecordCountCommand(collectionName, indexName));

        if (result.next()) {
            long count = result.getLong("n");
            getModel().setIndexRecordCount(indexName, count);
        }
    }

    /**
     * Method which collects index sizes for specified index
     * @param collectionName used collection
     * @param indexName used index
     * @throws DataCollectException when some QueryExecutionException occur during running help query
     */
    private void _collectIndexSizesData(String collectionName, String indexName) throws DataCollectException {
        CachedResult stats = executeQuery(MongoResources.getCollectionStatsCommand(collectionName));
        if (stats.next()) {
            int size = stats.getDocument("indexSizes").getInteger(indexName);
            getModel().setIndexByteSize(indexName, size);
            getModel().setIndexSizeInPages(indexName, (long)Math.ceil((double)size / getModel().getPageSize()));
        }
    }

    /**
     * Method which collects all index data about all used indexes
     * @param collectionName used collection
     * @throws DataCollectException when some QueryExecutionException occur during running help queries
     */
    private void _collectIndexesData(String collectionName) throws DataCollectException {
        for (String indexName : getModel().getIndexNames()) {
            _collectIndexSizesData(collectionName, indexName);
            _collectIndexRecordCount(collectionName, indexName);
        }
    }

    /**
     * Get collection used by query
     * @return collection name
     * @throws DataCollectException when no such collection was parsed from query
     */
    private String _getCollectionName() throws DataCollectException {
        for (String collectionName : getModel().getKindNames()) {
            return collectionName;
        }
        throw getExceptionsFactory(MongoExceptionsFactory.class).collectionNotParsed();
    }

    //Save Result data

    /**
     * Method which will save field data for field present in result
     * @param result result of executed main query
     */
    private void _collectResultAttributeData(ConsumedResult result) {
        for (String colName : result.getAttributeNames()) {
            for (String colType : result.getAttributeTypes(colName)) {
                if (colType != null) {
                    Integer size = MongoResources.DefaultSizes.getAvgColumnSizeByType(colType);
                    if (size != null)
                        getModel().setResultAttributeTypeByteSize(colName, colType, size);
                    double ratio = result.getAttributeTypeRatio(colName, colType);
                    getModel().setResultAttributeTypeRatio(colName, colType, ratio);
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
        getModel().setResultByteSize(size);
        long count = result.getRecordCount();
        getModel().setResultRecordCount(count);

        long sizeInPages = (long)Math.ceil((double) size / getModel().getPageSize());
        getModel().setResultSizeInPages(sizeInPages);

        _collectResultAttributeData(result);
    }

    /**
     *  Public method which collects all the statistical data for result
     * @param result result of main query
     */
    @Override
    public void collectData(ConsumedResult result) throws DataCollectException {
        String collName = _getCollectionName();
        _collectPageSize();
        _collectDatabaseData();
        _collectIndexesData(collName);
        _collectCollectionData(collName);
        _collectResultData(result);
    }
}
