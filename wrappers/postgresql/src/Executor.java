package wrappers.postgresql.src;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
//import java.sql.SQLException;
import java.sql.SQLException;

public class Executor {

    private String link;
    private Connection c;
    
    Executor(String link) {
        this.link = link;
    }

    private Statement initConnnection() {
        try {
            c = DriverManager.getConnection(link);
            return c.createStatement();
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

    public void executeQuery(String query) {
        try {
            Statement s = initConnnection();
            //TODO: gather data before query
            ResultSet result = s.executeQuery(query);

            printQueryResult(result);
            //TODO: gather data after query
            s.close();
            c.commit();
        }
        catch (Exception e) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
        }
    }
}
