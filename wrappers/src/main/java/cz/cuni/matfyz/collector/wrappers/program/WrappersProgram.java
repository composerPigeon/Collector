package cz.cuni.matfyz.collector.wrappers.program;

import com.fasterxml.jackson.core.JsonProcessingException;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import cz.cuni.matfyz.collector.wrappers.mongodb.MongoWrapper;
import cz.cuni.matfyz.collector.wrappers.neo4j.Neo4jWrapper;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresWrapper;

public class WrappersProgram {
    public static void main(String[] args) {

        try {
            MongoWrapper mongoWrapper = new MongoWrapper(
                    "localhost",
                    27017,
                    "test",
                    "",
                    ""
            );

            DataModel mongoModel = mongoWrapper.executeQuery("db.costumers.find()");
            System.out.println(mongoModel.toJson());

            Neo4jWrapper neo4jWrapper = new Neo4jWrapper(
                    "localhost",
                    7687,
                    "neo4j",
                    "neo4j",
                    "MiGWwErj5UxFfac"
            );

            DataModel neo4jModel = neo4jWrapper.executeQuery("MATCH (n) RETURN n;");
            System.out.println(neo4jModel.toJson());

            PostgresWrapper postgresWrapper = new PostgresWrapper(
                    "localhost",
                    5432,
                    "josefholubec",
                    "",
                    ""
            );

            DataModel postgresModel = postgresWrapper.executeQuery("SELECT * FROM fact_trendings;");
            System.out.println(postgresModel.toJson());

        } catch (WrapperException e) {
            e.printStackTrace();
        }

    }
}
