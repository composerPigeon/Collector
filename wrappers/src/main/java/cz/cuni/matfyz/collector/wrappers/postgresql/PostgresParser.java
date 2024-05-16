package cz.cuni.matfyz.collector.wrappers.postgresql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractParser;
import cz.cuni.matfyz.collector.wrappers.cachedresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;

public class PostgresParser extends AbstractParser<String, ResultSet> {
    private void _saveExecTime(Map<String, Object> root, DataModel dataModel) {
        Object result = root.get("Execution Time");
        if (result instanceof Double time) {
            dataModel.resultData().setExecutionTime(time);
        }
    }
    private void _parseTableName(Map<String, Object> node, DataModel dataModel) {
        if (node.get("Relation Name") instanceof String tableName) {
            dataModel.datasetData().addTable(tableName);
        }
    }

    private void _parseIndexName(Map<String, Object> node, DataModel dataModel) {
        if (node.get("Index Name") instanceof String relName) {
            dataModel.datasetData().addIndex(relName);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void _parseTree(Map<String, Object> root, DataModel dataModel) {
        if (root.containsKey("Execution Time")) {
            _saveExecTime(root, dataModel);
        }
        if (root.containsKey("Plan") && root.get("Plan") instanceof Map node) {
            _parseSubTree(node, dataModel);
        }
    }
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void _parseSubTree(Map<String, Object> root, DataModel dataModel) {
        if (root.get("Node Type") instanceof String nodeType) {
            if (nodeType.contains("Seq Scan")) {
                _parseTableName(root, dataModel);
            } else if (nodeType.contains("Index Scan")) {
                _parseIndexName(root, dataModel);
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
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void parseExplainTree(DataModel toModel, String explainTree) throws ParseException {
        try {
            ObjectMapper objectMapper = new ObjectMapper();

            List result = objectMapper.readValue(explainTree, List.class);

            for (Object plan: result) {
                if (plan instanceof Map root) {
                    _parseTree(root, toModel);
                }
            }
        } catch (JsonProcessingException e) {
            throw new ParseException(e);
        }
    }

    private void _addDataToBuilder(
            CachedResult.Builder builder,
            ResultSetMetaData metData,
            ResultSet resultSet
    ) throws SQLException {

        for (int i = 1; i <= metData.getColumnCount(); i++) {
            String columnName = metData.getColumnName(i);
            String className = metData.getColumnClassName(i);

            Object value;
            if (className.equals("java.lang.Double")) {
                value = resultSet.getDouble(i);
            } else if (className.equals("java.lang.Integer")) {
                value = resultSet.getInt(i);
            } else {
                value = resultSet.getString(i);
            }
            builder.toLastRecordAddValue(columnName, value);
        }
    }

    @Override
    public CachedResult parseResult(ResultSet resultSet) throws ParseException {
        try {
            var builder = new CachedResult.Builder();
            while (resultSet.next()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                builder.addEmptyRecord();
                _addDataToBuilder(builder, metaData, resultSet);
            }
            return builder.toResult();
        } catch (SQLException e) {
            throw new ParseException(e);
        }
    }

    // Parse Main Result
    private void _consumeDataToBuilder(ConsumedResult.Builder builder, ResultSetMetaData metaData, ResultSet resultSet) throws SQLException {
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i);
            String typeName = metaData.getColumnTypeName(i);

            builder.addColumnType(columnName, typeName);
        }
    }

    @Override
    public ConsumedResult parseMainResult(ResultSet resultSet, DataModel model) throws ParseException {
        try {
            var builder = new ConsumedResult.Builder();
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                builder.addRecord();
                _consumeDataToBuilder(builder, metaData, resultSet);
            }
            return builder.toResult();
        } catch (SQLException e) {
            throw new ParseException(e);
        }
    }

    @Override
    public ConsumedResult parseResultAndConsume(ResultSet resultSet) throws ParseException {
        try {
            var builder = new ConsumedResult.Builder();
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                builder.addRecord();
                _consumeDataToBuilder(builder, metaData, resultSet);
            }
            return builder.toResult();
        } catch (SQLException e) {
            throw new ParseException(e);
        }
    }
}
