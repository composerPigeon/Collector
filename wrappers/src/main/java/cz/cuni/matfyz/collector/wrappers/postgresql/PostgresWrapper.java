package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.resources.Queries;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.model.QueryData;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class PostgresWrapper extends AbstractWrapper {
    
    public PostgresWrapper(String link, String datasetName) throws SQLException {
        _link = link;
        _datasetName = datasetName;
        _connection = DriverManager.getConnection(link + "/" + datasetName);
        _connection.setAutoCommit(false);
    }

    private class QueryDataSaver {
        private final QueryData _queryData;
        private final Statement _statement;

        public QueryDataSaver(Statement statement, QueryData data) {
            _statement = statement;
            _queryData = data;
        }

        private void _saveDatasetSizeInPages(int size) throws SQLException {
            ResultSet result = _statement.executeQuery(Queries.Postgres.getPageSizeQuery());
            if (result.next()) {
                int pageSize = result.getInt("current_setting");
                int sizeInPages = (int) Math.ceil((double)size / (double)pageSize);
                _queryData.setDataSetSizeInPages(sizeInPages);
            }
        }
        public void saveDatabaseData() throws SQLException {
            ResultSet result = _statement.executeQuery(Queries.Postgres.getDatasetSizeQuery(_datasetName));
            if (result.next()) {
                int dataSetSize = result.getInt("pg_database_size");
                _queryData.setDataSetSize(dataSetSize);
                _saveDatasetSizeInPages(dataSetSize);
            }
        }

        private void _saveDistRatioForCol(String tableName, String colName) throws SQLException {
            ResultSet res = _statement.executeQuery(Queries.Postgres.getDistRatioColQuery(tableName, colName));
            if (res.next()) {
                double ratio = res.getDouble("n_distinct");
                _queryData.setColumnDistinctRatio(tableName, colName, ratio);
            }

        }
        private void _saveColSize(String tableName, String colName) throws SQLException {
            ResultSet result = _statement.executeQuery(Queries.Postgres.getColSizeQuery(tableName, colName));
            if (result.next()) {
                int size = result.getInt("avg_width");
                _queryData.setColumnByteSize(tableName, colName, size);
            }
        }
        private Set<String> _getColumnNames(String tableName) throws SQLException {
            ResultSet result = _statement.executeQuery(Queries.Postgres.getColNamesForTableQuery(tableName));
            Set<String> names = new HashSet<>();

            while (result.next()) {
                String name = result.getString("attname");
                names.add(name);
            }
            return names;
        }
        private void _saveColumnData(String tableName) throws SQLException{
            for (String columnName: _getColumnNames(tableName)) {
                _saveColSize(tableName, columnName);
                _saveDistRatioForCol(tableName, columnName);
            }
        }

        private void _saveTableSizeInPages(String tableName) throws SQLException {
            ResultSet result = _statement.executeQuery(Queries.Postgres.getTableSizeInPagesQuery(tableName));
            if (result.next()) {
                int sizeInPages = result.getInt("relpages");
                _queryData.setTableSizeInPages(tableName, sizeInPages);
            }
        }
        private void _saveTableSize(String tableName) throws SQLException {
            ResultSet result = _statement.executeQuery(Queries.Postgres.getTableSizeQuery(tableName));
            if (result.next()){
                int size = result.getInt("pg_total_relation_size");
                _queryData.setTableByteSize(tableName, size);
            }
        }
        private void _saveTableRowCount(String tableName) throws SQLException {
            ResultSet result = _statement.executeQuery(Queries.Postgres.getRowCountForTableQuery(tableName));
            if (result.next()) {
                int count = result.getInt("reltuples");
                _queryData.setTableRowCount(tableName, count);
            }
        }
        public void saveTableData(Set<String> tableNames) throws SQLException {
            for (String tableName: tableNames) {
                _saveTableSizeInPages(tableName);
                _saveTableSize(tableName);
                _saveTableRowCount(tableName);
                _saveColumnData(tableName);
            }
        }

        public void saveIndexData(Set<String> inxNames) {
            for (String indexName: inxNames) {
                //gather and save index data
            }
        }

    }

    private void _saveAfterQueryData(String query, DataModel dataModel) throws SQLException {
        Statement statement = _connection.createStatement();
        ResultSet result = statement.executeQuery(Queries.Postgres.getExplainPlanQuery(query));

        if (result.next()) {
            String jsonExplainPlan = result.getString("QUERY PLAN");
            PostgresExplainTreeParser.parseExplainTree(jsonExplainPlan, dataModel);
        }

        QueryDataSaver saver = new QueryDataSaver(statement, dataModel.afterQuery());
        saver.saveDatabaseData();
        saver.saveTableData(dataModel.getTableNames());
        saver.saveIndexData(dataModel.getIndexNames());
        //gather next data
    }

    private void _saveBeforeQueryData(DataModel dataModel) throws SQLException {
        Statement statement = _connection.createStatement();

        QueryDataSaver saver = new QueryDataSaver(statement, dataModel.beforeQuery());
        saver.saveDatabaseData();
        saver.saveTableData(dataModel.getTableNames());
        saver.saveIndexData(dataModel.getIndexNames());

    }

    @Override
    public DataModel executeQuery(String query) throws SQLException {
        //TODO: parse query to not contain explain plan
        DataModel dataModel = _parseQuery(query);

        _saveAfterQueryData(query, dataModel);
        _connection.rollback();
        _saveBeforeQueryData(dataModel);
        //TODO: gather data before query
        //connection.commit();

        return dataModel;
    }

    @Override
    protected DataModel _parseQuery(String query) {
        return new DataModel("PostgreSQL", _datasetName);
    }

    @Override
    public String toString() {
        return "Connection link: " + _link + "\n";
    }
}
