package wrappers.src.main.java.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import wrappers.src.main.java.abstractwrapper.AbstractWrapper;
import model.src.main.java.DataModel;

public class PostgresWrapper extends AbstractWrapper{
    
    public PostgresWrapper(String link) {
        this.link = link;
    }

    private Statement initConnnection() {
        try {
            connection = DriverManager.getConnection(link);
            return connection.createStatement();
        } catch (Exception e){
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            return null;
        }
    }

    private void printQueryResult(ResultSet result) throws SQLException {
        try {
            while (result.next()) {
                int id = result.getInt("id");
                String fname = result.getString("fname");
                String lname = result.getString("lname");

                System.out.println("id: " + id);
                System.out.println("fname: " + fname);
                System.out.println("lname: " + lname);
                System.out.println();
            }
        }
        catch (SQLException e) {
            throw new SQLException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public DataModel executeQuery(String query) throws SQLException {
        try {
            DataModel dataModel = parseQuery(query);
            //DataModel data = new DataModel();

            Statement s = initConnnection();
            //TODO: gather data before query
            ResultSet result = s.executeQuery(query);

            printQueryResult(result);
            //TODO: gather data after query
            s.close();
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


        return new DataModel(null);
    }

    @Override
    public String toString() {
        StringBuilder line = new StringBuilder();

        line.append("Connection link: " + link + "\n");

        return line.toString();
    }
}
