package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.model.DataModel;

import java.sql.*;

public class PostgresWrapper extends AbstractWrapper {
    
    public PostgresWrapper(String link, String datasetName) throws SQLException {
        _link = link;
        _datasetName = datasetName;
        _connection = DriverManager.getConnection(link + "/" + datasetName);
        _connection.setAutoCommit(false);
    }

    private void _saveDatabaseData(Statement statement, DataModel dataModel) throws SQLException {
        ResultSet result = statement.executeQuery("select pg_database_size('" + _datasetName + "')");
        if (result.next()) {
            int dataSetSize = result.getInt("pg_database_size");
            dataModel.afterQuery().setDataSetSize(dataSetSize);
        }
    }

    private void _saveColumnData(Statement statement, String tableName, DataModel dataModel) {

    }

    private void _saveTableData(Statement statement, DataModel dataModel) throws SQLException {
        for (String tableName: dataModel.afterQuery().getTableNames()) {
            // gather and save table data

            _saveColumnData(statement, tableName, dataModel);
        }
    }

    private void _saveIndexData(Statement statement, DataModel dataModel) {

    }

    private void _saveAfterQueryData(String query, DataModel dataModel) throws SQLException {
        Statement statement = _connection.createStatement();
        ResultSet result = statement.executeQuery("explain (analyze true, format json) " + query);

        if (result.next()) {
            String jsonExplainPlan = result.getString("QUERY PLAN");
            PostgresExplainTreeParser.parseExplainTree(jsonExplainPlan, dataModel);
        }

        _saveDatabaseData(statement, dataModel);
        //gather next data

        _connection.rollback();
    }

    private void _saveBeforeQueryData(DataModel dataModel) {

    }

    @Override
    public DataModel executeQuery(String query) throws SQLException {
        DataModel dataModel = _parseQuery(query);
        _saveAfterQueryData(query, dataModel);
        //TODO: gather data before query
        //connection.commit();

        return dataModel;
    }


    //Use Parser class for parsing query
    //Then get interesting items into data model from individual clauses of query
    //return the data model
    @Override
    protected DataModel _parseQuery(String query) {
        return new DataModel("PostgreSQL", _datasetName);
    }

    @Override
    public String toString() {
        return "Connection link: " + _link + "\n";
    }
}
