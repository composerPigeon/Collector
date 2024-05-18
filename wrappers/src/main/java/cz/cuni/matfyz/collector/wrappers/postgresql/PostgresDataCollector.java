package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.*;
import cz.cuni.matfyz.collector.wrappers.cachedresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

/**
 * Class which is responsible for collecting all statistical data and save them to data model
 */
class PostgresDataCollector extends AbstractDataCollector<String, ResultSet, String> {
    public PostgresDataCollector(PostgresConnection connection, DataModel model, String datasetName) {
        super(datasetName, model, connection);
    }

    //saving of dataset data

    /**
     * Method which saves page size to model
     * @throws QueryExecutionException when help query fails
     */
    private void _savePageSize() throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getPageSizeQuery());
        if (result.next()) {
            int pageSize = result.getInt("current_setting");
            _model.datasetData().setDataSetPageSize(pageSize);
        }
    }

    /**
     * Method which counts and saves dataset size in pages to model
     * @param size byte size of dataset
     */
    private void _saveDatasetSizeInPages(long size) {
        int pageSize = _model.getPageSize();
        if (pageSize > 0) {
            long sizeInPages = (long) Math.ceil((double)size / (double)pageSize);
            _model.datasetData().setDataSetSizeInPages(sizeInPages);
        }
    }

    /**
     * Method which saves sizes of dataset to model
     * @throws QueryExecutionException when help query fails
     */
    private void _saveDatasetDataSizes() throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getDatasetSizeQuery(_datasetName));
        if (result.next()) {
            long dataSetSize = result.getLong("pg_database_size");
            _model.datasetData().setDataSetSize(dataSetSize);
            _saveDatasetSizeInPages(dataSetSize);
        }
    }

    /**
     * Method which saves size of caches used by postgres and save it to model
     * @throws QueryExecutionException when help query fails
     */
    private void _saveDatasetCacheSize() throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getCacheSizeQuery());
        if (result.next()) {
            long size = result.getLong("shared_buffers");
            _model.datasetData().setDataSetCacheSize(size);
        }
    }

    /**
     * Method to save all dataset data to model
     * @throws QueryExecutionException when some of the help queries failed
     */
    private void _saveDatasetData() throws QueryExecutionException {
        _savePageSize();
        _saveDatasetDataSizes();
        _saveDatasetCacheSize();
    }

    //Saving of columns data

    /**
     * Method which saves data for specific column
     * @param tableName identify table
     * @param colName select column
     * @throws QueryExecutionException
     */
    private void _saveSpecificDataForCol(String tableName, String colName) throws QueryExecutionException {
        CachedResult res = _connection.executeQuery(PostgresResources.getColDataQuery(tableName, colName));
        if (res.next()) {
            double ratio = res.getDouble("n_distinct");
            int size = res.getInt("avg_width");
            _model.datasetData().setColumnDistinctRatio(tableName, colName, ratio);
            _model.datasetData().setColumnByteSize(tableName, colName, size);
        }

    }

    /**
     * Method which saves type and if column is mandatory (nullable)
     * @param tableName to specify table
     * @param colName to select column
     * @throws QueryExecutionException when help query fails
     */
    private void _saveTypeAndMandatoryForCol(String tableName, String colName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getColTypeAndMandatoryQuery(tableName, colName));
        if (result.next()) {
            String type = result.getString("typname");
            _model.datasetData().setColumnType(tableName, colName, type);

            boolean mandatory = result.getBoolean("attnotnull");
            _model.datasetData().setColumnMandatory(tableName, colName, mandatory);
        }
    }

    /**
     * Method which gets all column names for specific table
     * @param tableName to specify table
     * @return set of column names
     * @throws QueryExecutionException when help query fails
     */
    private Set<String> _getColumnNames(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getColNamesForTableQuery(tableName));
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
     * @throws QueryExecutionException when some of the help queries fails
     */
    private void _saveColumnData(String tableName) throws QueryExecutionException {
        for (String columnName: _getColumnNames(tableName)) {
            _saveSpecificDataForCol(tableName, columnName);
            _saveTypeAndMandatoryForCol(tableName, columnName);
        }
    }

    // Saving of tables data

    /**
     * Method which saves table row count to model
     * @param tableName to specify table
     * @throws QueryExecutionException when help query fails
     */
    private void _saveTableRowCount(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getRowCountForTableQuery(tableName));
        if (result.next()) {
            long rowCount = result.getLong("reltuples");
            _model.datasetData().setTableRowCount(tableName, rowCount);
        }
    }

    /**
     * Method which saves count of table constraints to model
     * @param tableName to specify table
     * @throws QueryExecutionException when help query fails
     */
    private void _saveTableConstraintCount(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getConstraintsCountForTableQuery(tableName));
        if (result.next()) {
            long count = result.getLong("relchecks");
            _model.datasetData().setTableConstraintCount(tableName, count);
        }
    }

    /**
     * Method which saves table size in pages ot model
     * @param tableName identify table
     * @throws QueryExecutionException when help query fails
     */
    private void _saveTableSizeInPages(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeInPagesQuery(tableName));
        if (result.next()) {
            long sizeInPages = result.getLong("relpages");
           _model.datasetData().setTableSizeInPages(tableName, sizeInPages);
        }
    }

    /**
     * Mathod which saves table size to model
     * @param tableName specifies table
     * @throws QueryExecutionException when help query fails
     */
    private void _saveTableSize(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeQuery(tableName));
        if (result.next()){
            long size = result.getLong("pg_total_relation_size");
            _model.datasetData().setTableByteSize(tableName, size);
        }
    }

    /**
     * Mehod for saving all table data
     * @throws QueryExecutionException when some of the help queries fails
     */
    private void _saveTableData() throws QueryExecutionException {
        for (String tableName : _model.getTableNames()) {
            _saveTableRowCount(tableName);
            _saveTableConstraintCount(tableName);
            _saveTableSizeInPages(tableName);
            _saveTableSize(tableName);
            _saveColumnData(tableName);
        }
    }

    //saving of index data

    /**
     * Method which saves table name for over which was built used index
     * @param indexName identify index
     * @throws QueryExecutionException when help query fails
     */
    private void _saveIndexTableName(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableNameForIndexQuery(indexName));
        if (result.next()) {
            String tableName = result.getString("tablename");
            _model.datasetData().addTable(tableName);
        }
    }

    /**
     * Method which saves index row count to model
     * @param indexName index identifier
     * @throws QueryExecutionException when help query fails
     */
    private void _saveIndexRowCount(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getRowCountForTableQuery(indexName));
        if (result.next()) {
            long rowCount = result.getLong("reltuples");
            _model.datasetData().setIndexRowCount(indexName, rowCount);
        }
    }

    /**
     * Method for saving index size in pages to data model
     * @param indexName to specify index
     * @throws QueryExecutionException when help query fails
     */
    private void _saveIndexSizeInPages(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeInPagesQuery(indexName));
        if (result.next()) {
            long sizeInPages = result.getLong("relpages");
            _model.datasetData().setIndexSizeInPages(indexName, sizeInPages);
        }
    }

    /**
     * Method for saving index size to data model
     * @param indexName to specify index
     * @throws QueryExecutionException when help query fails
     */
    private void _saveIndexSize(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeQuery(indexName));
        if (result.next()) {
            long size = result.getLong("pg_total_relation_size");
            _model.datasetData().setIndexByteSize(indexName, size);
        }
    }

    /**
     * Method for saving all index data
     * @throws QueryExecutionException when some of the help queries fails
     */
    private void _saveIndexData() throws QueryExecutionException {
        for (String indexName: _model.getIndexNames()) {
            _saveIndexTableName(indexName);
            _saveIndexRowCount(indexName);
            _saveIndexSizeInPages(indexName);
            _saveIndexSize(indexName);
        }
    }


    /**
     * Method which gets table name for column based on its name and type
     * @param columnName specified column name
     * @param columnType specified type
     * @return corresponding table name
     * @throws QueryExecutionException when some of the help queries failed
     * @throws DataCollectException when no table for some column was found
     */
    private String _getTableNameForColumn(String columnName, String columnType) throws QueryExecutionException, DataCollectException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableNameForColumnQuery(columnName, columnType));
        while (result.next()) {
            String tableName = result.getString("relname");
            if (_model.getTableNames().contains(tableName)) {
                return tableName;
            }
        }
        throw new DataCollectException("No Table for ColumnName " + columnName + " was found");
    }

    /**
     * Method which saves statistics about the main result
     * @param mainResult main result for which we want to save stats
     * @throws QueryExecutionException when some of the help queries failed
     * @throws DataCollectException when no table for some column was found
     */
    private void _saveResultData(ConsumedResult mainResult) throws QueryExecutionException, DataCollectException {
        long rowCount = mainResult.getRowCount();
        _model.resultData().setRowCount(rowCount);

        long sizeInBytes = 0;
        for (String columnName : mainResult.getColumnNames()) {
            String colType = mainResult.getColumnType(columnName);
            String tableName = _getTableNameForColumn(columnName, colType);
            int colSize = _model.getColumnByteSize(tableName, columnName);
            sizeInBytes += colSize;
            _model.resultData().setColumnByteSize(columnName, colSize);
            _model.resultData().setColumnType(columnName, colType);
        }
        sizeInBytes *= rowCount;

        _model.resultData().setByteSize(sizeInBytes);
        int pageSize = _model.getPageSize();
        if (pageSize > 0)
            _model.resultData().setSizeInPages((int)Math.ceil((double) sizeInBytes / pageSize));
    }

    /**
     * Public method which collects all statistical data after main query execution
     * @param result result of main query for which will wrapper collects all the data
     * @return instance of DataModel
     * @throws DataCollectException when some help queries failed
     */
    @Override
    public DataModel collectData(ConsumedResult result) throws DataCollectException {
        try {
            _saveDatasetData();
            _saveIndexData();
            _saveTableData();
            _saveResultData(result);
            return _model;
        } catch (QueryExecutionException e) {
            throw new DataCollectException(e);
        }

    }

}
