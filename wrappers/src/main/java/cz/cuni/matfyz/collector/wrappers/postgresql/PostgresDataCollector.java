package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.*;
import cz.cuni.matfyz.collector.wrappers.queryresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import cz.cuni.matfyz.collector.wrappers.queryresult.CachedResult;

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
    private void _collectPageSize() throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getPageSizeQuery());
        if (result.next()) {
            int pageSize = result.getInt("current_setting");
            _model.dataset().setDataSetPageSize(pageSize);
        }
    }

    /**
     * Method which counts and saves dataset size in pages to model
     * @param size byte size of dataset
     */
    private void _collectDatasetSizeInPages(long size) {
        int pageSize = _model.getPageSize();
        if (pageSize > 0) {
            long sizeInPages = (long) Math.ceil((double)size / (double)pageSize);
            _model.dataset().setDataSetSizeInPages(sizeInPages);
        }
    }

    /**
     * Method which saves sizes of dataset to model
     * @throws QueryExecutionException when help query fails
     */
    private void _collectDatasetDataSizes() throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getDatasetSizeQuery(_datasetName));
        if (result.next()) {
            long dataSetSize = result.getLong("pg_database_size");
            _model.dataset().setDataSetSize(dataSetSize);
            _collectDatasetSizeInPages(dataSetSize);
        }
    }

    /**
     * Method which saves size of caches used by postgres and save it to model
     * @throws QueryExecutionException when help query fails
     */
    private void _collectDatasetCacheSize() throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getCacheSizeQuery());
        if (result.next()) {
            long size = result.getLong("shared_buffers");
            _model.dataset().setDataSetCacheSize(size);
        }
    }

    /**
     * Method to save all dataset data to model
     * @throws QueryExecutionException when some of the help queries failed
     */
    private void _collectDatasetData() throws QueryExecutionException {
        _collectPageSize();
        _collectDatasetDataSizes();
        _collectDatasetCacheSize();
    }

    //Saving of columns data

    /**
     * Method which saves data for specific column
     * @param tableName identify table
     * @param colName select column
     * @throws QueryExecutionException when help query fails
     */
    private void _collectNumericDataForCol(String tableName, String colName, String typeName) throws QueryExecutionException {
        CachedResult res = _connection.executeQuery(PostgresResources.getColDataQuery(tableName, colName));
        if (res.next()) {
            double ratio = res.getDouble("n_distinct");
            int size = res.getInt("avg_width");
            _model.dataset().setColumnDistinctRatio(tableName, colName, ratio);
            _model.dataset().setColumnTypeByteSize(tableName, colName, typeName, size);
        }

    }

    /**
     * Method which saves type and if column is mandatory (nullable)
     * @param tableName to specify table
     * @param colName to select column
     * @throws QueryExecutionException when help query fails
     */
    private void _collectTypeAndMandatoryForCol(String tableName, String colName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getColTypeAndMandatoryQuery(tableName, colName));
        if (result.next()) {
            String type = result.getString("typname");
            _collectNumericDataForCol(tableName, colName, type);

            boolean mandatory = result.getBoolean("attnotnull");
            _model.dataset().setColumnMandatory(tableName, colName, mandatory);
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
    private void _collectColumnData(String tableName) throws QueryExecutionException {
        for (String columnName: _getColumnNames(tableName)) {
            _collectTypeAndMandatoryForCol(tableName, columnName);
        }
    }

    // Saving of tables data

    /**
     * Method which saves table row count to model
     * @param tableName to specify table
     * @throws QueryExecutionException when help query fails
     */
    private void _collectTableRowCount(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getRowCountForTableQuery(tableName));
        if (result.next()) {
            long rowCount = result.getLong("reltuples");
            _model.dataset().setTableRowCount(tableName, rowCount);
        }
    }

    /**
     * Method which saves count of table constraints to model
     * @param tableName to specify table
     * @throws QueryExecutionException when help query fails
     */
    private void _collectTableConstraintCount(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getConstraintsCountForTableQuery(tableName));
        if (result.next()) {
            long count = result.getLong("relchecks");
            _model.dataset().setTableConstraintCount(tableName, count);
        }
    }

    /**
     * Method which saves table size in pages ot model
     * @param tableName identify table
     * @throws QueryExecutionException when help query fails
     */
    private void _collectTableSizeInPages(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeInPagesQuery(tableName));
        if (result.next()) {
            long sizeInPages = result.getLong("relpages");
           _model.dataset().setTableSizeInPages(tableName, sizeInPages);
        }
    }

    /**
     * Mathod which saves table size to model
     * @param tableName specifies table
     * @throws QueryExecutionException when help query fails
     */
    private void _collectTableSize(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeQuery(tableName));
        if (result.next()){
            long size = result.getLong("pg_total_relation_size");
            _model.dataset().setTableByteSize(tableName, size);
        }
    }

    /**
     * Mehod for saving all table data
     * @throws QueryExecutionException when some of the help queries fails
     */
    private void _collectTableData() throws QueryExecutionException {
        for (String tableName : _model.getTableNames()) {
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
     * @param indexName identify index
     * @throws QueryExecutionException when help query fails
     */
    private void _collectIndexTableName(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableNameForIndexQuery(indexName));
        if (result.next()) {
            String tableName = result.getString("tablename");
            _model.dataset().addTable(tableName);
        }
    }

    /**
     * Method which saves index row count to model
     * @param indexName index identifier
     * @throws QueryExecutionException when help query fails
     */
    private void _collectIndexRowCount(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getRowCountForTableQuery(indexName));
        if (result.next()) {
            long rowCount = result.getLong("reltuples");
            _model.dataset().setIndexRowCount(indexName, rowCount);
        }
    }

    /**
     * Method for saving index size in pages to data model
     * @param indexName to specify index
     * @throws QueryExecutionException when help query fails
     */
    private void _collectIndexSizeInPages(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeInPagesQuery(indexName));
        if (result.next()) {
            long sizeInPages = result.getLong("relpages");
            _model.dataset().setIndexSizeInPages(indexName, sizeInPages);
        }
    }

    /**
     * Method for saving index size to data model
     * @param indexName to specify index
     * @throws QueryExecutionException when help query fails
     */
    private void _collectIndexSize(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeQuery(indexName));
        if (result.next()) {
            long size = result.getLong("pg_total_relation_size");
            _model.dataset().setIndexByteSize(indexName, size);
        }
    }

    /**
     * Method for saving all index data
     * @throws QueryExecutionException when some of the help queries fails
     */
    private void _collectIndexData() throws QueryExecutionException {
        for (String indexName: _model.getIndexNames()) {
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
    private void _collectResultData(ConsumedResult mainResult) throws QueryExecutionException, DataCollectException {
        long rowCount = mainResult.getRowCount();
        _model.result().setRowCount(rowCount);

        long sizeInBytes = 0;
        double colSize = 0;
        for (String columnName : mainResult.getColumnNames()) {
            colSize = 0;
            for (String colType : mainResult.getColumnTypes(columnName)) {
                String tableName = _getTableNameForColumn(columnName, colType);
                int typeSize = _model.dataset().getColumnTypeByteSize(tableName, columnName, colType);
                _model.result().setColumnTypeByteSize(columnName, colType, typeSize);
                double ratio = mainResult.getColumnTypeRatio(columnName, colType);
                _model.result().setColumnTypeRatio(columnName, colType, ratio);
                colSize += typeSize * ratio;
            }
            sizeInBytes += Math.round(colSize);
        }
        sizeInBytes *= rowCount;
        _model.result().setByteSize(sizeInBytes);

        int pageSize = _model.getPageSize();
        if (pageSize > 0)
            _model.result().setSizeInPages((int)Math.ceil((double) sizeInBytes / pageSize));
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
            _collectDatasetData();
            _collectIndexData();
            _collectTableData();
            _collectResultData(result);
            return _model;
        } catch (QueryExecutionException e) {
            throw new DataCollectException(e);
        }

    }

}
