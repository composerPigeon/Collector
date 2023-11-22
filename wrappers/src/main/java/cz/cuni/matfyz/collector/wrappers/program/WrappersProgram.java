package cz.cuni.matfyz.collector.wrappers.program;

import com.fasterxml.jackson.core.JsonProcessingException;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresWrapper;

import java.sql.SQLException;

public class WrappersProgram {
    public static void main(String[] args) {
        try {
            PostgresWrapper wrapper = new PostgresWrapper("jdbc:postgresql://localhost:5432", "josefholubec");

            DataModel model = wrapper.executeQuery("select * from knows_view");

            System.out.println(model.toJson());
        }
        catch (SQLException | JsonProcessingException e) {
            System.err.println(e.getMessage());
        }

    }
}
