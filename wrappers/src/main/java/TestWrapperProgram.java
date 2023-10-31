package wrappers.src.main.java;

import wrappers.src.main.java.postgresql.PostgresWrapper;

import java.sql.SQLException;

public class TestWrapperProgram {
    public static void main(String[] args) {
        try {
            PostgresWrapper wrapper = new PostgresWrapper("jdbc:postgresql://localhost:5432/josefholubec");

            wrapper.executeQuery("select * from person");
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        //PostgresWrapper wrapper = new PostgresWrapper("jdbc:postgresql://localhost:5432/josefholubec");
        
    }
}
