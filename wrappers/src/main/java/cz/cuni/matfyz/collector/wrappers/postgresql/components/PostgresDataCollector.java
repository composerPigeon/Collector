package cz.cuni.matfyz.collector.wrappers.postgresql.components;

import cz.cuni.matfyz.collector.model.DataModelException;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.AbstractDataCollector;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.AbstractQueryResultParser;
import cz.cuni.matfyz.collector.wrappers.exceptions.ConnectionException;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresExceptionsFactory;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresResources;
import cz.cuni.matfyz.collector.wrappers.queryresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.queryresult.CachedResult;

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

/**
 * Class which is responsible for collecting all statistical data and save them to data model
 */
public class PostgresDataCollector extends AbstractDataCollector<ResultSet, String, String> {
    public PostgresDataCollector(
            AbstractWrapper.ExecutionContext<ResultSet, String, String> context,
            AbstractQueryResultParser<ResultSet> resultParser
    ) throws ConnectionException {
        super(context, resultParser);
    }

    //saving of database data

    /**
     * Method which saves page size to model
     * @throws DataCollectException when help query fails
     */
    private void _collectPageSize() throws DataCollectException {
        CachedResult result = executeQuery(PostgresResources.getPageSizeQuery());
        if (result.next()) {
            int pageSize = result.getInt("current_setting");
            getModel().setPageSize(pageSize);
        }
    }

    /**
     * Method which counts and saves dataset size in pages to model
     * @param size byte size of dataset
     */
    private void _collectDatabaseSizeInPages(long size) {
        int pageSize = getModel().getPageSize();
        if (pageSize > 0) {
            long sizeInPages = (long) Math.ceil((double)size / (double)pageSize);
            getModel().setDatabaseSizeInPages(sizeInPages);
        }
    }

    /**
     * Method which saves sizes of dataset to model
     * @throws DataCollectException when help query fails
     */
    private void _collectDatabaseDataSizes() throws DataCollectException {
        CachedResult result = executeQuery(PostgresResources.getDatabaseSizeQuery(getDatabaseName()));
        if (result.next()) {
            long dataSetSize = result.getLong("pg_database_size");
            getModel().setDatabaseByteSize(dataSetSize);
            _collectDatabaseSizeInPages(dataSetSize);
        }
    }
    
    
    /**
     * Method which saves size of caches used by postgres and save it to model
     * @throws DataCollectException when help query fails
     */
    private void _collectDatabaseCacheSize() throws DataCollectException {
        CachedResult result = executeQuery(PostgresResources.getCacheSizeQuery());
        if (result.next()) {
            long size = result.getLong("shared_buffers");
            getModel().setDatabaseCacheSize(size);
        }
    }

    /**
     * Method to save all dataset data to model
     * @throws DataCollectException when some of the help queries failed
     */
    private void _collectDatabaseData() throws DataCollectException {
        _collectPageSize();
        _collectDatabaseDataSizes();
        _collectDatabaseCacheSize();
    }

    //Saving of columns data

    /**
     * Method which saves data for specific column
     * @param tableName identify table
     * @param colName select column
     * @throws DataCollectException when help query fails
     */
    private void _collectNumericDataForCol(String tableName, String colName, String typeName) throws DataCollectException {
        CachedResult res = executeQuery(PostgresResources.getColByteSizeQuery(tableName, colName));
        if (res.next()) {
            int size = res.getInt("avg_width");
            getModel().setAttributeTypeByteSize(tableName, colName, typeName, size);
            getModel().setAttributeTypeRatio(tableName, colName, typeName, 1);
        }

        res = executeQuery(PostgresResources.getColDistinctValuesCountQuery(tableName, colName));

        if (res.next()) {
            long distinctValuesCount = res.getLong("count");
            getModel().setAttributeDistinctValuesCount(tableName, colName, distinctValuesCount);
        }

    }

    /**
     * Method which saves type and if column is mandatory (nullable)
     * @param tableName to specify table
     * @param colName to select column
     * @throws DataCollectException when help query fails
     */
    private void _collectTypeAndMandatoryForCol(String tableName, String colName) throws DataCollectException {
        CachedResult result = executeQuery(PostgresResources.getColTypeAndMandatoryQuery(tableName, colName));
        if (result.next()) {
            String type = result.getString("typname");
            _collectNumericDataForCol(tableName, colName, type);

            boolean mandatory = result.getBoolean("attnotnull");
            getModel().setAttributeMandatory(tableName, colName, mandatory);
        }
    }

    /**
     * Method which gets all column names for specific table
     * @param tableName to specify table
     * @return set of column names
     * @throws DataCollectException when help query fails
     */
    private Set<String> _getColumnNames(String tableName) throws DataCollectException {
        CachedResult result = executeQuery(PostgresResources.getColNamesForTableQuery(tableName));
        Set<String> names = new HashSet<>();

        while (result.next()) {
            String name = result.getString("attname");
            names.add(name);
        }
        return names;
    }

    /**
     * Method which saves all column data for some table
     * @param tableName to specify table
     * @throws DataCollectException when some of the help queries fails
     */
    private void _collectColumnData(String tableName) throws DataCollectException {
        for (String columnName: _getColumnNames(tableName)) {
            _collectTypeAndMandatoryForCol(tableName, columnName);
        }
    }

    // Saving of tables data

    /**
     * Method which saves table row count to model
     * @param tableName to specify table
     * @throws DataCollectException when help query fails
     */
    private void _collectTableRowCount(String tableName) throws DataCollectException {
        CachedResult result = executeQuery(PostgresResources.getRelationRecordCountQuery(tableName));
        if (result.next()) {
            long rowCount = result.getLong("reltuples");
            getModel().setKindRecordCount(tableName, rowCount);
        }
    }

    /**
     * Method which saves count of table constraints to model
     * @param tableName to specify table
     * @throws DataCollectException when help query fails
     */
    private void _collectTableConstraintCount(String tableName) throws DataCollectException {
        CachedResult result = executeQuery(PostgresResources.getConstraintsCountForTableQuery(tableName));
        if (result.next()) {
            int count = result.getInt("relchecks");
            getModel().setKindConstraintCount(tableName, count);
        }
    }

    /**
     * Method which saves table size in pages ot model
     *
     * @param tableName identify table
     * @throws DataCollectException when help query fails
     */
    private void _collectTableSizeInPages(String tableName) throws DataCollectException {
        CachedResult result = executeQuery(PostgresResources.getRelationSizeInPagesQuery(tableName));
        if (result.next()) {
            long sizeInPages = result.getLong("relpages");
            getModel().setKindSizeInPages(tableName, sizeInPages);
        }
    }

    /**
     * Method which saves table size to model
     *
     * @param tableName specifies table
     * @throws DataCollectException when help query fails
     */
    private void _collectTableSize(String tableName) throws DataCollectException {
        CachedResult result = executeQuery(PostgresResources.getRelationSizeQuery(tableName));
        if (result.next()) {
            long size = result.getLong("pg_total_relation_size");
            getModel().setKindByteSize(tableName, size);
        }
    }

    /**
     * Method for saving all table data
     * @throws DataCollectException when some of the help queries fails
     */
    private void _collectTableData() throws DataCollectException {
        for (String tableName : getModel().getKindNames()) {
            _collectTableRowCount(tableName);
            _collectTableConstraintCount(tableName);
            _collectTableSizeInPages(tableName);
            _collectTableSize(tableName);
            _collectColumnData(tableName);
        }
    }

    //saving of index data

    /**
     * Method which saves table name for over which was built used index
     *
     * @param indexName identify index
     * @throws DataCollectException when help query fails
     */
    private void _collectIndexTableName(String indexName) throws DataCollectException {
        CachedResult result = executeQuery(PostgresResources.getTableNameForIndexQuery(indexName));
        if (result.next()) {
            String tableName = result.getString("tablename");
            getModel().addKind(tableName);
        }
    }

    /**
     * Method which saves index row count to model
     *
     * @param indexName index identifier
     * @throws DataCollectException when help query fails
     */
    private void _collectIndexRowCount(String indexName) throws DataCollectException {
        CachedResult result = executeQuery(PostgresResources.getRelationRecordCountQuery(indexName));
        if (result.next()) {
            long rowCount = result.getLong("reltuples");
            getModel().setIndexRecordCount(indexName, rowCount);
        }
    }

    /**
     * Method for saving index size in pages to data model
     *
     * @param indexName to specify index
     * @throws DataCollectException when help query fails
     */
    private void _collectIndexSizeInPages(String indexName) throws DataCollectException {
        CachedResult result = executeQuery(PostgresResources.getRelationSizeInPagesQuery(indexName));
        if (result.next()) {
            long sizeInPages = result.getLong("relpages");
            getModel().setIndexSizeInPages(indexName, sizeInPages);
        }
    }

    /**
     * Method for saving index size to data model
     *
     * @param indexName to specify index
     * @throws DataCollectException when help query fails
     */
    private void _collectIndexSize(String indexName) throws DataCollectException {
        CachedResult result = executeQuery(PostgresResources.getRelationSizeQuery(indexName));
        if (result.next()) {
            long size = result.getLong("pg_total_relation_size");
            getModel().setIndexByteSize(indexName, size);
        }
    }

    /**
     * Method for saving all index data
     * @throws DataCollectException when some of the help queries fails
     */
    private void _collectIndexData() throws DataCollectException {
        for (String indexName: getModel().getIndexNames()) {
            _collectIndexTableName(indexName);
            _collectIndexRowCount(indexName);
            _collectIndexSizeInPages(indexName);
            _collectIndexSize(indexName);
        }
    }


    /**
     * Method which gets table name for column based on its name and type
     * @param columnName specified column name
     * @param columnType specified type
     * @return corresponding table name
     * @throws DataCollectException when no table for some column was found
     */
    private String _getTableNameForColumn(String columnName, String columnType) throws DataCollectException {
        CachedResult result = executeQuery(PostgresResources.getTableNameForColumnQuery(columnName, columnType));
        while (result.next()) {
            String tableName = result.getString("relname");
            if (getModel().getKindNames().contains(tableName)) {
                return tableName;
            }
        }
        throw getExceptionsFactory(PostgresExceptionsFactory.class).tableForColumnNotFound(columnName);
    }

    /**
     * Method which saves statistics about the main result
     * @param mainResult main result for which we want to save stats
     * @throws DataCollectException when no table for some column was found
     */
    private void _collectResultData(ConsumedResult mainResult) throws DataCollectException {
        long rowCount = mainResult.getRecordCount();
        getModel().setResultRecordCount(rowCount);

        long sizeInBytes = 0;
        double colSize = 0;
        for (String columnName : mainResult.getAttributeNames()) {
            colSize = 0;
            for (String colType : mainResult.getAttributeTypes(columnName)) {
                try {
                    String tableName = _getTableNameForColumn(columnName, colType);
                    int typeSize = getModel().getAttributeTypeByteSize(tableName, columnName, colType);
                    getModel().setResultAttributeTypeByteSize(columnName, colType, typeSize);
                    double ratio = mainResult.getAttributeTypeRatio(columnName, colType);
                    getModel().setResultAttributeTypeRatio(columnName, colType, ratio);
                    colSize += typeSize * ratio;
                } catch (DataModelException e) {
                    throw getExceptionsFactory(PostgresExceptionsFactory.class).byteSizeForColumnTypeNotFoundInDataModel(columnName, colType);
                }
            }
            sizeInBytes += Math.round(colSize);
        }
        sizeInBytes *= rowCount;
        getModel().setResultByteSize(sizeInBytes);

        int pageSize = getModel().getPageSize();
        if (pageSize > 0)
            getModel().setResultSizeInPages((int) Math.ceil((double) sizeInBytes / pageSize));
    }

    /**
     * Public method which collects all statistical data after main query execution
     * @param result result of main query for which will wrapper collects all the data
     * @throws DataCollectException when some help queries failed
     */
    @Override
    public void collectData(ConsumedResult result) throws DataCollectException {
        _collectDatabaseData();
        _collectIndexData();
        _collectTableData();
        _collectResultData(result);
    }

}
