package cz.cuni.matfyz.collector.wrappers.neo4j;

import java.time.LocalDate;
import java.time.ZonedDateTime;

public class Neo4jResources {
    public static final String DATABASE_NAME = "Neo4j";
    public static String getExplainPlanQuery(String query) {
        return "profile " + query;
    }
    public static String getNodesQuery(String nodeLabel) { return "match (n:" + nodeLabel + ") return *;"; }

    public static String getRelationsQuery(String edgeLabel) { return "match ()-[e:" + edgeLabel + "]->() return e;"; }
    public static String getIndexDataQuery(String indexType, String label, String property) {
        return "show indexes yield name, type, labelsOrTypes, properties where type = \"" + indexType + "\" and \"" + label + "\" in labelsOrTypes and \"" + property + "\" in properties;";
    }
    public static String getNodeAndPropertyQuery(String nodeLabel, String propertyName) {
        return "match (n:" + nodeLabel + ") return n." + propertyName  + ";";
    }
    public static String getEdgeAndPropertyQuery(String edgeLabel, String propertyName) {
        return "match ()-[e:" + edgeLabel + "]->() return e." + propertyName  + ";";
    }
    public static String getConstraintCountForLabelQuery(String label) {
        return "show constraints yield labelsOrTypes where \"" + label + "\" in labelsOrTypes return count(*) as count;";
    }

    public static String getAllNodesQuery() {
        return "match (n) return n;";
    }
    public static String getAllRelationsQuery() {
        return "match ()-[e]->() return e;";
    }

    public static String getPageCacheSizeQuery() {
        return "show settings yield name, value where name=\"server.memory.pagecache.size\";";
    }

    public static class DefaultSizes {
        public static int getAvgColumnSize(Object object) {
            if (object instanceof Integer || object instanceof Double || object instanceof Boolean || object instanceof LocalDate || object instanceof ZonedDateTime)
                return 41;
            else
                return 128;
        }

        public static int NODE_SIZE = 15;
        public static int EDGE_SIZE = 34;
        public static int PAGE_SIZE = 8192;
    }
}
