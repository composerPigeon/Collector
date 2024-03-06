package cz.cuni.matfyz.collector.wrappers.postgresql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractParser;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.ExplainParseException;

public class PostgresParser extends AbstractParser<String> {
    public PostgresParser(String datasetName) {
        super(datasetName);
    }

    private void _saveExecTime(Map<String, Object> root, DataModel dataModel) {
        Object result = root.get("Execution Time");
        if (result instanceof Double time) {
            dataModel.toResultData().setExecutionTime(time);
        }
    }
    private void _saveTableData(Map<String, Object> node, DataModel dataModel) {
        if (node.get("Relation Name") instanceof String relName) {
            if (node.get("Actual Rows") instanceof Integer rowCount) {
                dataModel.toDatasetData().setTableRowCount(relName, rowCount);
            }
        }
    }

    private void _saveIndexData(Map<String, Object> node, DataModel dataModel) {

    }

    private void _parseTree(Map<String, Object> root, DataModel dataModel) {
        if (root.containsKey("Execution Time")) {
            _saveExecTime(root, dataModel);
        }
        if (root.containsKey("Plan") && root.get("Plan") instanceof Map node) {
            _parseSubTree(node, dataModel);
        }
    }

    private void _parseSubTree(Map<String, Object> root, DataModel dataModel) {
        if (root.get("Node Type") instanceof String nodeType) {
            if ("Seq Scan".equals(nodeType)) {
                _saveTableData(root, dataModel);
            } else if ("Index Scan".equals(nodeType)) {
                _saveIndexData(root, dataModel);
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

    @Override
    public DataModel parseExplainTree(String forQuery, String jsonExplainTree) throws ExplainParseException {
        try {
            DataModel dataModel = new DataModel(forQuery, PostgresResources.DATABASE_NAME, _datasetName);
            ObjectMapper objectMapper = new ObjectMapper();

            List result = objectMapper.readValue(jsonExplainTree, List.class);

            for (Object plan: result) {
                if (plan instanceof Map root) {
                    _parseTree(root, dataModel);
                }
            }

            return dataModel;
        } catch (JsonProcessingException e) {
            throw new ExplainParseException(e);
        }
    }
}
