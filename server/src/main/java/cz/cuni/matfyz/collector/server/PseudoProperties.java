package cz.cuni.matfyz.collector.server;

import java.util.HashSet;
import java.util.Set;

public class PseudoProperties {
    private static final Set<DbInstance> _instances = new HashSet<>();

    public static void loadDbInstances() {
        _instances.add(new DbInstance(
                DbType.Neo4j,
                "neo4j",
                "bolt://localhost:7687",
                "neo4j",
                "neo4j",
                "MiGWwErj5UxFfac"
        ));
        _instances.add(new DbInstance(
                DbType.PostgreSQL,
                "postgres",
                "jdbc:postgresql://localhost:5432",
                "josefholubec",
                "",
                ""
        ));
    }

    public static Set<DbInstance> getDbInstances() {
        return _instances;
    }

    public record DbInstance(
            DbType dbType,
            String instanceName,
            String hostName,
            String datasetName,
            String userName,
            String password
    ) {}

    public enum DbType {
        Neo4j,
        PostgreSQL,
        MongoDB
    }
}
