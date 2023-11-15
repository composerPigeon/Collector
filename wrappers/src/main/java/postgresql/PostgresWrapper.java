package wrappers.src.main.java.postgresql;

//import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
//import java.sql.Statement;
import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import wrappers.src.main.java.abstractwrapper.AbstractWrapper;
import model.src.main.java.DataModel;

public class PostgresWrapper extends AbstractWrapper{

    private class Parser {

        public static DataModel parseExplainTree(String jsonExplainTree) {
            try {
                DataModel dataModel = new DataModel("PostrgeSQL", "");

                ObjectMapper objectMapper = new ObjectMapper();

                Object resultMap = objectMapper.readValue(jsonExplainTree, Object.class);

                //resultMap.keySet().forEach(key -> {System.out.println(key);});

                System.out.println(resultMap);

                return dataModel;
            }
            catch (Exception e) {
                System.out.println(e);
                return null;
            }
        }
    }
    
    public PostgresWrapper(String link) throws SQLException{
        try {
            this.link = link;
            connection = DriverManager.getConnection(link);
        }
        catch (SQLException e) {
            throw e;
        }
    }

    private void printQueryResult(ResultSet result) throws SQLException {
        try {
            System.out.println();
            while (result.next()) {
                String subFname = result.getString("He_fname");
                String subLname = result.getString("He_lname");
                String obFname = result.getString("Who_fname");
                String obLanme = result.getString("Who_lname");

                System.out.println(subFname + " " + subLname + " knows " + obFname + " " + obLanme);
                System.out.println();
            }
        }
        catch (SQLException e) {
            throw e;
        }
    }

    private void printExplainRes(ResultSet result) throws SQLException {
        try {
            while(result.next()) {
                String plan = result.getString("QUERY PLAN");

                System.out.println("Query plan:");
                System.out.println(plan);
            }
        }
        catch (SQLException e) {
            throw e;
        }
    }

    @Override
    public DataModel executeQuery(String query) throws SQLException {
        try {
            DataModel dataModel = parseQuery(query);
            //DataModel data = new DataModel();

            PreparedStatement resStmt = connection.prepareStatement(query);
            PreparedStatement planStmt = connection.prepareStatement("explain (analyze true, format json) " + query);
            //TODO: gather data before query
            ResultSet res = resStmt.executeQuery();
            ResultSet planRes = planStmt.executeQuery();

            Parser.parseExplainTree(planRes.getString("QUERY PLAN"));

            printQueryResult(res);
            printExplainRes(planRes);
            //TODO: gather data after query
            //connection.commit();

            return dataModel;
        }
        catch (SQLException e) {
            throw e;
        }
    }


    //Use Parser class for parsing query
    //Thean get interesting items into data model from individual clauses of query
    //return the data model
    @Override
    protected DataModel parseQuery(String query) {


        return new DataModel("PostgreSQL", "");
    }

    @Override
    public String toString() {
        StringBuilder line = new StringBuilder();

        line.append("Connection link: " + link + "\n");

        return line.toString();
    }
}
