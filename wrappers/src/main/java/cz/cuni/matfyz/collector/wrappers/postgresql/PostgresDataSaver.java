package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractDataSaver;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.DataSaveException;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.QueryExecutionException;

import javax.xml.crypto.Data;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class PostgresDataSaver extends AbstractDataSaver<String, ResultSet> {
    private final PostgresConnection _connection;
    public PostgresDataSaver(PostgresConnection connection, String datasetName) {
        super(datasetName);
        _connection = connection;
    }

    //saving of dataset data
    private void _savePageSize(DataModel toModel) throws QueryExecutionException, SQLException {
        ResultSet result = _connection.executeQuery(PostgresResources.getPageSizeQuery());
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
    public void _saveDatasetData(DataModel toModel) throws QueryExecutionException, SQLException {
        ResultSet result = _connection.executeQuery(PostgresResources.getDatasetSizeQuery(_datasetName));
        if (result.next()) {
            int dataSetSize = result.getInt("pg_database_size");
            toModel.toDatasetData().setDataSetSize(dataSetSize);
            _saveDatasetSizeInPages(dataSetSize, toModel);
        }
    }

    //Saving of tables data
    private void _saveDistRatioForCol(String tableName, String colName, DataModel toModel) throws QueryExecutionException, SQLException {
        ResultSet res = _connection.executeQuery(PostgresResources.getDistRatioColQuery(tableName, colName));
        if (res.next()) {
            double ratio = res.getDouble("n_distinct");
            toModel.toDatasetData().setColumnDistinctRatio(tableName, colName, ratio);
        }

    }
    private void _saveColSize(String tableName, String colName, DataModel toModel) throws QueryExecutionException, SQLException {
        ResultSet result = _connection.executeQuery(PostgresResources.getColSizeQuery(tableName, colName));
        if (result.next()) {
            int size = result.getInt("avg_width");
            toModel.toDatasetData().setColumnByteSize(tableName, colName, size);
        }
    }
    private Set<String> _getColumnNames(String tableName) throws QueryExecutionException, SQLException {
        ResultSet result = _connection.executeQuery(PostgresResources.getColNamesForTableQuery(tableName));
        Set<String> names = new HashSet<>();

        while (result.next()) {
            String name = result.getString("attname");
            names.add(name);
        }
        return names;
    }
    private void _saveColumnData(String tableName, DataModel toModel) throws QueryExecutionException, SQLException{
        for (String columnName: _getColumnNames(tableName)) {
            _saveColSize(tableName, columnName, toModel);
            _saveDistRatioForCol(tableName, columnName, toModel);
        }
    }

    private void _saveTableSizeInPages(String tableName, DataModel toModel) throws QueryExecutionException, SQLException {
        ResultSet result = _connection.executeQuery(PostgresResources.getTableSizeInPagesQuery(tableName));
        if (result.next()) {
            int sizeInPages = result.getInt("relpages");
           toModel.toDatasetData().setTableSizeInPages(tableName, sizeInPages);
        }
    }
    private void _saveTableSize(String tableName, DataModel toModel) throws QueryExecutionException, SQLException {
        ResultSet result = _connection.executeQuery(PostgresResources.getTableSizeQuery(tableName));
        if (result.next()){
            int size = result.getInt("pg_total_relation_size");
            toModel.toDatasetData().setTableByteSize(tableName, size);
        }
    }
    private void _saveTableRowCount(String tableName, DataModel toModel) throws QueryExecutionException, SQLException {
        ResultSet result = _connection.executeQuery(PostgresResources.getRowCountForTableQuery(tableName));
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
    private  int _getResultRowSizeInBytes(DataModel dataModel, ResultSetMetaData metaData) throws SQLException {
        int rowSize = 0;
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            String colName = metaData.getColumnName(i);
            String tableName = metaData.getTableName(i);
            rowSize += dataModel.toDatasetData().getColumnByteSize(tableName, colName);
        }
        return rowSize;
    }

    private void _saveResultSizeInBytes(DataModel toModel, int sizeInBytes) throws SQLException {
        toModel.toResultData().setByteSize(sizeInBytes);
    }

    private int _saveResultRowCount(DataModel toModel, ResultSet result) throws SQLException {
        result.beforeFirst();
        result.last();
        int count = result.getRow();

        toModel.toResultData().setRowCount(count);
        return count;
    }

    private void _saveResultData(DataModel toModel) throws DataSaveException {
        try {
            ResultSet result = _connection.getMainQueryResult();
            ResultSetMetaData metaData = result.getMetaData();

            int rowCount = _saveResultRowCount(toModel, result);

            int rowSize = _getResultRowSizeInBytes(toModel, metaData);
            int sizeInBytes = rowSize * rowCount;
            toModel.toResultData().setByteSize(sizeInBytes);

            int pageSize = toModel.getPageSize();
            if (pageSize > 0)
                toModel.toResultData().setSizeInPages((int)Math.ceil((double) sizeInBytes / pageSize));


            result.close();
        } catch (SQLException e) {
            throw new DataSaveException(e);
        }
    }

    @Override
    public void saveDataTo(DataModel dataModel) throws DataSaveException {
        try {
            _savePageSize(dataModel);
            _saveDatasetData(dataModel);
            _saveTableData(dataModel);
            _saveIndexData(dataModel);
            _saveResultData(dataModel);
        } catch (QueryExecutionException | SQLException e) {
            throw new DataSaveException(e);
        }

    }

}
