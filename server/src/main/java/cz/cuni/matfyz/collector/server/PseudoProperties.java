package cz.cuni.matfyz.collector.server;

public class PseudoProperties {
    public static class Noe4j {
        public static final String INSTANCE_NAME = "neo4j";
        public static final String HOST = "bolt://localhost:7687";
        public static final String DATASET_NAME = "neo4j";
        public static final String USER = "neo4j";
        public static final String PASSWORD = "MiGWwErj5UxFfac";
    }

    public static class Postgres {
        public static final String INSTANCE_NAME = "Postgres";
        public static final String HOST = "jdbc:postgresql://localhost:5432";
        public static final String DATASET_NAME = "josefholubec";
        public static final String USER = "";
        public static final String PASSWORD = "";
    }
}
