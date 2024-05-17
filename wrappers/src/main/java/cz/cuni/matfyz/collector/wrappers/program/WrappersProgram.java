package cz.cuni.matfyz.collector.wrappers.program;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import cz.cuni.matfyz.collector.wrappers.mongodb.MongoWrapper;
import cz.cuni.matfyz.collector.wrappers.neo4j.Neo4jWrapper;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WrappersProgram {

    private static Logger _logger = LoggerFactory.getLogger(WrappersProgram.class);
    private static void mongoTests() throws WrapperException {
        MongoWrapper mongoWrapper = new MongoWrapper(
                "localhost",
                27017,
                "test",
                "",
                ""
        );

        DataModel mongoModel = mongoWrapper.executeQuery("db.costumers.find()");
        System.out.println(mongoModel.toJson());

        //mongoModel = mongoWrapper.executeQuery("db.costumers.find().count()");
        //System.out.println(mongoModel.toJson());

        mongoModel = mongoWrapper.executeQuery("db.costumers.find({}, {customer_id: 1, customer_name: 1})");
        System.out.println(mongoModel.toJson());

        mongoModel = mongoWrapper.executeQuery("db.costumers.find({\"customer_id\": { \"$gt\": 30 }})");
        System.out.println(mongoModel.toJson());
    }
    public static void main(String[] args) {

        try {

            mongoTests();

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
            _logger.error(e.getMessage(), e);
        }

    }
}
