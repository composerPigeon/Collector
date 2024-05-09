package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.*;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

class PostgresDataCollector extends AbstractDataCollector<String, ResultSet, String> {
    public PostgresDataCollector(PostgresConnection connection, DataModel model, String datasetName) {
        super(datasetName, model, connection);
    }

    //saving of dataset data
    private void _savePageSize() throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getPageSizeQuery());
        if (result.next()) {
            int pageSize = result.getInt("current_setting");
            _model.datasetData().setDataSetPageSize(pageSize);
        }
    }
    private void _saveDatasetSizeInPages(long size) {
        int pageSize = _model.getPageSize();
        if (pageSize > 0) {
            long sizeInPages = (long) Math.ceil((double)size / (double)pageSize);
            _model.datasetData().setDataSetSizeInPages(sizeInPages);
        }
    }
    private void _saveDatasetDataSizes() throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getDatasetSizeQuery(_datasetName));
        if (result.next()) {
            long dataSetSize = result.getLong("pg_database_size");
            _model.datasetData().setDataSetSize(dataSetSize);
            _saveDatasetSizeInPages(dataSetSize);
        }
    }
    private void _saveDatasetCacheSize() throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getCacheSizeQuery());
        if (result.next()) {
            long size = result.getLong("shared_buffers");
            _model.datasetData().setDataSetCacheSize(size);
        }
    }
    private void _saveDatasetData() throws QueryExecutionException {
        _savePageSize();
        _saveDatasetDataSizes();
        _saveDatasetCacheSize();
    }

    //Saving of columns data
    private void _saveSpecificDataForCol(String tableName, String colName) throws QueryExecutionException {
        CachedResult res = _connection.executeQuery(PostgresResources.getColDataQuery(tableName, colName));
        if (res.next()) {
            double ratio = res.getDouble("n_distinct");
            int size = res.getInt("avg_width");
            _model.datasetData().setColumnDistinctRatio(tableName, colName, ratio);
            _model.datasetData().setColumnByteSize(tableName, colName, size);
            _model.datasetData().setColumnMandatory(tableName, colName, true);
        }

    }
    private void _saveTypeForCol(String tableName, String colName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getColTypeQuery(tableName, colName));
        if (result.next()) {
            String type = result.getString("typname");
            _model.datasetData().setColumnType(tableName, colName, type);
        }
    }

    private Set<String> _getColumnNames(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getColNamesForTableQuery(tableName));
        Set<String> names = new HashSet<>();

        while (result.next()) {
            String name = result.getString("attname");
            names.add(name);
        }
        return names;
    }
    private void _saveColumnData(String tableName) throws QueryExecutionException {
        for (String columnName: _getColumnNames(tableName)) {
            _saveSpecificDataForCol(tableName, columnName);
            _saveTypeForCol(tableName, columnName);
        }
    }

    // Saving of tables data
    private void _saveTableRowCount(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getRowCountForTableQuery(tableName));
        if (result.next()) {
            long rowCount = result.getLong("reltuples");
            _model.datasetData().setTableRowCount(tableName, rowCount);
        }
    }
    private void _saveTableConstraintCount(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getConstraintsCountForTableQuery(tableName));
        if (result.next()) {
            long count = result.getLong("relchecks");
            _model.datasetData().setTableConstraintCount(tableName, count);
        }
    }
    private void _saveTableSizeInPages(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeInPagesQuery(tableName));
        if (result.next()) {
            long sizeInPages = result.getLong("relpages");
           _model.datasetData().setTableSizeInPages(tableName, sizeInPages);
        }
    }
    private void _saveTableSize(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeQuery(tableName));
        if (result.next()){
            long size = result.getLong("pg_total_relation_size");
            _model.datasetData().setTableByteSize(tableName, size);
        }
    }
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
    private void _saveIndexTableName(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableNameForIndexQuery(indexName));
        if (result.next()) {
            String tableName = result.getString("tablename");
            _model.datasetData().addTable(tableName);
        }
    }
    private void _saveIndexRowCount(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getRowCountForTableQuery(indexName));
        if (result.next()) {
            long rowCount = result.getLong("reltuples");
            _model.datasetData().setIndexRowCount(indexName, rowCount);
        }
    }
    private void _saveIndexSizeInPages(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeInPagesQuery(indexName));
        if (result.next()) {
            long sizeInPages = result.getLong("relpages");
            _model.datasetData().setIndexSizeInPages(indexName, sizeInPages);
        }
    }
    private void _saveIndexSize(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeQuery(indexName));
        if (result.next()) {
            long size = result.getLong("pg_total_relation_size");
            _model.datasetData().setIndexByteSize(indexName, size);
        }
    }
    private void _saveIndexData() throws QueryExecutionException {
        for (String indexName: _model.getIndexNames()) {
            _saveIndexTableName(indexName);
            _saveIndexRowCount(indexName);
            _saveIndexSizeInPages(indexName);
            _saveIndexSize(indexName);
        }
    }



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

    private void _saveResultData(CachedResult mainResult) throws QueryExecutionException, DataCollectException {
        int rowCount = mainResult.getRowCount();
        _model.resultData().setRowCount(rowCount);

        int sizeInBytes = 0;
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

    @Override
    public DataModel collectData(CachedResult result) throws DataCollectException {
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
