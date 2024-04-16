package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.*;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.neo4j.Neo4jResources;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

class PostgresDataCollector extends AbstractDataCollector<String, ResultSet> {
    public PostgresDataCollector(PostgresConnection connection, DataModel model, String datasetName) {
        super(datasetName, model, connection);
    }

    //saving of dataset data
    private void _savePageSize() throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getPageSizeQuery());
        if (result.next()) {
            int pageSize = result.getInt("current_setting");
            _model.toDatasetData().setDataSetPageSize(pageSize);
        }
    }
    private void _saveDatasetSizeInPages(int size) {
        int pageSize = _model.getPageSize();
        if (pageSize > 0) {
            int sizeInPages = (int) Math.ceil((double)size / (double)pageSize);
            _model.toDatasetData().setDataSetSizeInPages(sizeInPages);
        }
    }
    private void _saveDatasetDataSizes() throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getDatasetSizeQuery(_datasetName));
        if (result.next()) {
            int dataSetSize = result.getInt("pg_database_size");
            _model.toDatasetData().setDataSetSize(dataSetSize);
            _saveDatasetSizeInPages(dataSetSize);
        }
    }
    private void _saveDatasetCacheSize() throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getCacheSizeQuery());
        if (result.next()) {
            int size = result.getInt("shared_buffers");
            _model.toDatasetData().setDataSetCacheSize(size);
        }
    }
    private void _saveDatasetData() throws QueryExecutionException {
        _savePageSize();
        _saveDatasetDataSizes();
        _saveDatasetCacheSize();
    }

    //Saving of columns data
    private void _saveDistRatioForCol(String tableName, String colName) throws QueryExecutionException {
        CachedResult res = _connection.executeQuery(PostgresResources.getDistRatioColQuery(tableName, colName));
        if (res.next()) {
            double ratio = res.getDouble("n_distinct");
            _model.toDatasetData().setColumnDistinctRatio(tableName, colName, ratio);
        }

    }
    private void _saveColSize(String tableName, String colName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getColSizeQuery(tableName, colName));
        if (result.next()) {
            int size = result.getInt("avg_width");
            _model.toDatasetData().setColumnByteSize(tableName, colName, size);
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
            _saveColSize(tableName, columnName);
            _saveDistRatioForCol(tableName, columnName);
        }
    }

    // Saving of tables data
    private void _saveTableRowCount(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getRowCountForTableQuery(tableName));
        if (result.next()) {
            int rowCount = result.getInt("reltuples");
            _model.toDatasetData().setTableRowCount(tableName, rowCount);
        }
    }
    private void _saveTableConstraintCount(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getConstraintsCountForTableQuery(tableName));
        if (result.next()) {
            int count = result.getInt("relchecks");
            _model.toDatasetData().setTableConstraintCount(tableName, count);
        }
    }
    private void _saveTableSizeInPages(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeInPagesQuery(tableName));
        if (result.next()) {
            int sizeInPages = result.getInt("relpages");
           _model.toDatasetData().setTableSizeInPages(tableName, sizeInPages);
        }
    }
    private void _saveTableSize(String tableName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeQuery(tableName));
        if (result.next()){
            int size = result.getInt("pg_total_relation_size");
            _model.toDatasetData().setTableByteSize(tableName, size);
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
            _model.toDatasetData().addTable(tableName);
        }
    }
    private void _saveIndexRowCount(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getRowCountForTableQuery(indexName));
        if (result.next()) {
            int rowCount = result.getInt("reltuples");
            _model.toDatasetData().setIndexRowCount(indexName, rowCount);
        }
    }
    private void _saveIndexSizeInPages(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeInPagesQuery(indexName));
        if (result.next()) {
            int sizeInPages = result.getInt("relpages");
            _model.toDatasetData().setIndexSizeInPages(indexName, sizeInPages);
        }
    }
    private void _saveIndexSize(String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeQuery(indexName));
        if (result.next()) {
            int size = result.getInt("pg_total_relation_size");
            _model.toDatasetData().setIndexByteSize(indexName, size);
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
        _model.toResultData().setRowCount(rowCount);

        int sizeInBytes = 0;
        for (String columnName : mainResult.getColumnNames()) {
            String tableName = _getTableNameForColumn(columnName, mainResult.getColumnType(columnName));
            sizeInBytes += _model.getColumnByteSize(tableName, columnName);
        }
        sizeInBytes *= rowCount;

        _model.toResultData().setByteSize(sizeInBytes);
        int pageSize = _model.getPageSize();
        if (pageSize > 0)
            _model.toResultData().setSizeInPages((int)Math.ceil((double) sizeInBytes / pageSize));
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
