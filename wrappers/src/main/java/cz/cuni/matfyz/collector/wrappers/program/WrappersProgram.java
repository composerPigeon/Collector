package cz.cuni.matfyz.collector.wrappers.program;

import com.fasterxml.jackson.core.JsonProcessingException;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import cz.cuni.matfyz.collector.wrappers.neo4j.Neo4jWrapper;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresWrapper;

public class WrappersProgram {
    public static void main(String[] args) {
        try {
            //localhost:27017

            Neo4jWrapper neo4jWrapper = new Neo4jWrapper("bolt://localhost:7687/neo4j", "neo4j");

            DataModel neo4jModel = neo4jWrapper.executeQuery("MATCH (nineties:Movie) WHERE nineties.released >= 1990 AND nineties.released < 2000 RETURN nineties.title");

            System.out.println(neo4jModel.toJson());

            //try (Neo4JConnection nativeConnection = new Neo4JConnection("bolt://localhost:7687/neo4j", "neo4j", "MiGWwErj5UxFfac")) {
            //    System.out.println(nativeConnection.executeQueryWithExplain("MATCH (n:Train) return n"));
            //}

            PostgresWrapper postgresWrapper = new PostgresWrapper("jdbc:postgresql://localhost:5432", "josefholubec");

            DataModel postgresModel = postgresWrapper.executeQuery("select * from fact_trendings where videoid = 'I6hswz4rIrU'");

            System.out.println(postgresModel.toJson());
        }
        catch (WrapperException | JsonProcessingException e) {
            e.printStackTrace();
        }

    }
}
