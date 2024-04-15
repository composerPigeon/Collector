package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.*;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.exceptions.DataSaveException;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.exceptions.QueryExecutionException;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.cachedresult.MainCachedResult;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

class PostgresDataSaver extends AbstractDataSaver {
    private final PostgresConnection _connection;
    public PostgresDataSaver(PostgresConnection connection, String datasetName) {
        super(datasetName);
        _connection = connection;
    }

    //saving of dataset data
    private void _savePageSize(DataModel toModel) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getPageSizeQuery());
        if (result.next()) {
            int pageSize = result.getInt("current_setting");
            toModel.toDatasetData().setDataSetPageSize(pageSize);
        }
    }
    private void _saveDatasetSizeInPages(int size, DataModel toModel) {
        int pageSize = toModel.getPageSize();
        if (pageSize > 0) {
            int sizeInPages = (int) Math.ceil((double)size / (double)pageSize);
            toModel.toDatasetData().setDataSetSizeInPages(sizeInPages);
        }
    }
    public void _saveDatasetData(DataModel toModel) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getDatasetSizeQuery(_datasetName));
        if (result.next()) {
            int dataSetSize = result.getInt("pg_database_size");
            toModel.toDatasetData().setDataSetSize(dataSetSize);
            _saveDatasetSizeInPages(dataSetSize, toModel);
        }
    }

    //Saving of tables data
    private void _saveDistRatioForCol(String tableName, String colName, DataModel toModel) throws QueryExecutionException {
        CachedResult res = _connection.executeQuery(PostgresResources.getDistRatioColQuery(tableName, colName));
        if (res.next()) {
            double ratio = res.getDouble("n_distinct");
            toModel.toDatasetData().setColumnDistinctRatio(tableName, colName, ratio);
        }

    }
    private void _saveColSize(String tableName, String colName, DataModel toModel) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getColSizeQuery(tableName, colName));
        if (result.next()) {
            int size = result.getInt("avg_width");
            toModel.toDatasetData().setColumnByteSize(tableName, colName, size);
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
    private void _saveColumnData(String tableName, DataModel toModel) throws QueryExecutionException {
        for (String columnName: _getColumnNames(tableName)) {
            _saveColSize(tableName, columnName, toModel);
            _saveDistRatioForCol(tableName, columnName, toModel);
        }
    }

    private void _saveTableSizeInPages(String tableName, DataModel toModel) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeInPagesQuery(tableName));
        if (result.next()) {
            int sizeInPages = result.getInt("relpages");
           toModel.toDatasetData().setTableSizeInPages(tableName, sizeInPages);
        }
    }
    private void _saveTableSize(String tableName, DataModel toModel) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getTableSizeQuery(tableName));
        if (result.next()){
            int size = result.getInt("pg_total_relation_size");
            toModel.toDatasetData().setTableByteSize(tableName, size);
        }
    }
    private void _saveTableRowCount(String tableName, DataModel toModel) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(PostgresResources.getRowCountForTableQuery(tableName));
        if (result.next()) {
            int count = result.getInt("reltuples");
            toModel.toDatasetData().setTableRowCount(tableName, count);
        }
    }
    private void _saveTableData(DataModel toModel) throws QueryExecutionException, SQLException {
        for (String tableName : toModel.getTableNames()) {
            _saveTableSizeInPages(tableName, toModel);
            _saveTableSize(tableName, toModel);
            _saveTableRowCount(tableName, toModel);
            _saveColumnData(tableName, toModel);
        }
    }

    //saving of index data
    private void _saveIndexData(DataModel toModel) {
        for (String indexName: toModel.getIndexNames()) {
            //gather and save index data
        }
    }


    //saving of result data
    private  int _getResultRowSizeInBytes(MainCachedResult result) throws SQLException {
        int rowSize = 0;
        for (int i = 0; i < result.getColumnCount(); i++) {
            rowSize += result.getColumnSize(i);
        }
        return rowSize;
    }

    private void _saveResultData(MainCachedResult result, DataModel model) throws DataSaveException {
        try {
            int rowCount = result.getRowCount();
            model.toResultData().setRowCount(rowCount);

            int rowSize = _getResultRowSizeInBytes(result);
            int sizeInBytes = rowSize * rowCount;
            model.toResultData().setByteSize(sizeInBytes);

            int pageSize = model.getPageSize();
            if (pageSize > 0)
                model.toResultData().setSizeInPages((int)Math.ceil((double) sizeInBytes / pageSize));

        } catch (SQLException e) {
            throw new DataSaveException(e);
        }
    }

    @Override
    public void saveData(MainCachedResult result, DataModel dataModel) throws DataSaveException {
        try {
            _savePageSize(dataModel);
            _saveDatasetData(dataModel);
            _saveTableData(dataModel);
            _saveIndexData(dataModel);
            _saveResultData(result, dataModel);
        } catch (QueryExecutionException | SQLException e) {
            throw new DataSaveException(e);
        }

    }

}
