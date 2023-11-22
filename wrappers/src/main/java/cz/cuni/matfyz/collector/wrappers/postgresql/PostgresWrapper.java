package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.model.DataModel;

import java.sql.*;

public class PostgresWrapper extends AbstractWrapper {
    
    public PostgresWrapper(String link, String datasetName) throws SQLException {
        this.link = link;
        this.datasetName = datasetName;
        connection = DriverManager.getConnection(link + "/" + datasetName);
        connection.setAutoCommit(false);
    }

    private void saveDatabaseData(Statement statement, DataModel dataModel) throws SQLException {
        ResultSet result = statement.executeQuery("select pg_database_size('" + datasetName + "')");
        if (result.next()) {
            int dataSetSize = result.getInt("pg_database_size");
            dataModel.afterQuery().setDataSetSize(dataSetSize);
        }
    }

    private void saveColumnData(Statement statement, String tableName, DataModel dataModel) {

    }

    private void saveTableData(Statement statement, DataModel dataModel) throws SQLException {
        for (String tableName: dataModel.afterQuery().getTableNames()) {
            // gather and save table data

            saveColumnData(statement, tableName, dataModel);
        }
    }

    private void saveIndexData(Statement statement, DataModel dataModel) {

    }

    private void saveAfterQueryData(String query, DataModel dataModel) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("explain (analyze true, format json) " + query);

        if (result.next()) {
            String jsonExplainPlan = result.getString("QUERY PLAN");
            PostgresExplainTreeParser.parseExplainTree(jsonExplainPlan, dataModel);
        }

        saveDatabaseData(statement, dataModel);
        //gather next data

        connection.rollback();
    }

    private void saveBeforeQueryData(DataModel dataModel) {

    }

    @Override
    public DataModel executeQuery(String query) throws SQLException {
        //DataModel dataModel = parseQuery(query);
        DataModel dataModel = new DataModel("PostrgreSQL", datasetName);
        saveAfterQueryData(query, dataModel);
        //TODO: gather data before query
        //connection.commit();

        return dataModel;
    }


    //Use Parser class for parsing query
    //Then get interesting items into data model from individual clauses of query
    //return the data model
    @Override
    protected DataModel parseQuery(String query) {


        return new DataModel("PostgreSQL", datasetName);
    }

    @Override
    public String toString() {
        return "Connection link: " + link + "\n";
    }
}
