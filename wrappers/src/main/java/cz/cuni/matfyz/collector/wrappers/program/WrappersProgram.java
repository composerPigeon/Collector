package cz.cuni.matfyz.collector.wrappers.program;

import com.fasterxml.jackson.core.JsonProcessingException;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.WrapperException;
import cz.cuni.matfyz.collector.wrappers.neo4j.Neo4jWrapper;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresWrapper;

import java.sql.SQLException;

public class WrappersProgram {
    public static void main(String[] args) {
        try {
            //localhost:27017

            Neo4jWrapper neo4jWrapper = new Neo4jWrapper("jdbc:neo4j:bolt://localhost:7687", "neo4j");

            DataModel neo4jModel = neo4jWrapper.executeQuery("MATCH (n:Train) return n");

            System.out.println(neo4jModel.toJson());

            //try (Neo4JConnection nativeConnection = new Neo4JConnection("bolt://localhost:7687/neo4j", "neo4j", "MiGWwErj5UxFfac")) {
            //    System.out.println(nativeConnection.executeQueryWithExplain("MATCH (n:Train) return n"));
            //}

            PostgresWrapper postgresWrapper = new PostgresWrapper("jdbc:postgresql://localhost:5432", "josefholubec");

            DataModel postgresModel = postgresWrapper.executeQuery("select * from knows_view");

            System.out.println(postgresModel.toJson());
        }
        catch (WrapperException | JsonProcessingException e) {
            e.printStackTrace();
        }

    }
}
