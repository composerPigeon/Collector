package cz.cuni.matfyz.collector.wrappers.postgresql;

public abstract class PostgresResources {
    public static final String DATABASE_NAME = "PostgreSQL";
    public static String getExplainPlanQuery(String query) {
        return "explain (analyze true, format json) " + query;
    }
    public static String getDatasetSizeQuery(String datasetName) {
        return "select pg_database_size('" + datasetName + "')";
    }
    public static String getPageSizeQuery() {
        return "select current_setting('block_size')";
    }


    public static String getTableSizeInPagesQuery(String tableName) {
        return "select relpages from pg_class where relname = '" + tableName + "';";
    }
    public static String getTableSizeQuery(String tableName) {
        return "select pg_total_relation_size('" + tableName + "');";
    }
    public static String getConstraintsCountForTableQuery(String tableName) {
        return "select relcheck from pg_class where relname '" + tableName + "';";
    }
    public static String getRowCountForTableQuery(String tableName) {
        return "select reltuples from pg_class where relname = '" + tableName + "';";
    }
    public static String getColNamesForTableQuery(String tableName) {
        return "select attname from pg_stats where tablename = '" + tableName + "';";
    }

    public static String getDistRatioColQuery(String tableName, String colName) {
        return "select n_distinct from pg_stats where tablename = '" + tableName + "' and attname = '" + colName + "';";
    }
    public static String getColSizeQuery(String tableName, String colName) {
        return "select avg_width from pg_stats where tablename = '" + tableName + "' and attname = '" + colName + "';";
    }
}
