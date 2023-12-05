package cz.cuni.matfyz.collector.wrappers.postgresql;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

import cz.cuni.matfyz.collector.model.DataModel;

public class PostgresExplainTreeParser {

    private static void _saveExecTime(Map<String, Object> root, DataModel dataModel) {
        Object result = root.get("Execution Time");
        if (result instanceof Double time) {
            dataModel.setExecTime(time);
        }
    }
    private static void _saveTableData(Map<String, Object> node, DataModel dataModel) {
        if (node.get("Relation Name") instanceof String relName) {
            if (node.get("Actual Rows") instanceof Integer rowCount) {
                dataModel.afterQuery().setTableRowCount(relName, rowCount);
            }
        }
    }

    private static void _parseTree(Map<String, Object> root, DataModel dataModel) {
        if (root.containsKey("Execution Time")) {
            _saveExecTime(root, dataModel);
        }
        if (root.containsKey("Plan") && root.get("Plan") instanceof Map node) {
            _parseSubTree(node, dataModel);
        }
    }

    private static void _parseSubTree(Map<String, Object> root, DataModel dataModel) {
        if (root.get("Node Type") instanceof String nodeType) {
            if ("Seq Scan".equals(nodeType)) {
                _saveTableData(root, dataModel);
            } else if ("Index Scan".equals(nodeType)) {

            }

            if (root.containsKey("Plans") && root.get("Plans") instanceof List list) {
                for(Object o: list) {
                    if (o instanceof Map node) {
                        _parseSubTree(node, dataModel);
                    }
                }
            }
        }
    }

    public static void parseExplainTree(String jsonExplainTree, DataModel dataModel) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();

            List<Object> result = objectMapper.readValue(jsonExplainTree, List.class);

            for (Object plan: result) {
                if (plan instanceof Map root) {
                    _parseTree(root, dataModel);
                }
            }

            //resultMap.keySet().forEach(System.out::println);
            System.out.println(result);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
