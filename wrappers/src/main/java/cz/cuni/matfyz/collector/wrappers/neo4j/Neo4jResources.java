package cz.cuni.matfyz.collector.wrappers.neo4j;

public class Neo4jResources {
    public static final String DATABASE_NAME = "Neo4j";
    public static String getExplainPlanQuery(String query) {
        return "PROFILE " + query;
    }
    public static String getNodesQuery(String nodeLabel) { return "MATCH (n:" + nodeLabel + ") RETURN *;"; }

    public static String getRelationsQuery(String edgeLabel) { return "MATCH ()-[e:" + edgeLabel + "]->() RETURN e;"; }

    public static class DefaultSizes {
        public static int getAvgColumnSize(Object object) {
            if (object instanceof Integer || object instanceof Double || object instanceof Boolean)
                return 41;
            else
                return 128;
        }

        public static int NODE_SIZE = 15;
        public static int EDGE_SIZE = 34;
        public static int PAGE_SIZE = 8192;
    }
}
