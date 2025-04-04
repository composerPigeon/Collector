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
import cz.cuni.matfyz.collector.wrappers.queryresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.queryresult.CachedResult;

/**
 * Class which is responsible for parsing native results and explain plan result
 */
public class PostgresParser extends AbstractParser<String, ResultSet> {

    /**
     * Method which saves the execution time of query to model
     * @param root explain trees root
     * @param model model to save data
     */
    private void _saveExecTime(Map<String, Object> root, DataModel model) {
        Object result = root.get("Execution Time");
        if (result instanceof Double time) {
            model.setResultExecutionTime(time);
        }
    }

    /**
     * Method which parser table names from explain tree to model
     * @param node explain trees node
     * @param model model to save data
     */
    private void _parseTableName(Map<String, Object> node, DataModel model) {
        if (node.get("Relation Name") instanceof String tableName) {
            model.addTable(tableName);
        }
    }

    /**
     * Method which parses index names from explain tree to model
     * @param node explain trees node
     * @param model model to save data
     */
    private void _parseIndexName(Map<String, Object> node, DataModel model) {
        if (node.get("Index Name") instanceof String relName) {
            model.addIndex(relName);
        }
    }

    /**
     * Method which parses tho root of the explain tree
     * @param root root of the explain tree
     * @param model model to save data
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void _parseTree(Map<String, Object> root, DataModel model) {
        if (root.containsKey("Execution Time")) {
            _saveExecTime(root, model);
        }
        if (root.containsKey("Plan") && root.get("Plan") instanceof Map node) {
            _parseSubTree(node, model);
        }
    }

    /**
     * Method which recursively parses the subtree of explain result
     * @param root actual node of explain tree to be parsed
     * @param model model to save important data
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void _parseSubTree(Map<String, Object> root, DataModel model) {
        if (root.get("Node Type") instanceof String nodeType) {
            if (nodeType.contains("Seq Scan")) {
                _parseTableName(root, model);
            } else if (nodeType.contains("Index Scan")) {
                _parseIndexName(root, model);
            }

            if (root.containsKey("Plans") && root.get("Plans") instanceof List list) {
                for(Object o: list) {
                    if (o instanceof Map node) {
                        _parseSubTree(node, model);
                    }
                }
            }
        }
    }


    /**
     * Method which parse explain tree and important data saves to DataModel
     * @param toModel instance of DataModel where collected information are stored
     * @param explainTree explain tree to be parsed
     * @throws ParseException when JsonProcessingException occurs during the process
     */
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

    /**
     * Method which adds values to cached result and parse correctly parse them using metaData
     * @param builder builder to accumulate all values
     * @param metData metaData to get type information about columns
     * @param resultSet native result of parsed query
     * @throws SQLException when sql exception occur
     */
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

    /**
     * Method for parsing ordinal result to CachedResult
     * @param resultSet result of some query
     * @return instance of CachedResult
     * @throws ParseException when SQLException occurs during the process
     */
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

    /**
     * Method which is responsible to consume column types and column names of result
     * @param builder builder to accumulate these information and then build correct result
     * @param metaData metadata object used for getting column info
     * @param resultSet the main result
     * @throws SQLException from accessing metadata
     */
    private void _consumeColumnDataToBuilder(ConsumedResult.Builder builder, ResultSetMetaData metaData, ResultSet resultSet) throws SQLException {
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i);
            String typeName = metaData.getColumnTypeName(i);

            builder.addColumnType(columnName, typeName);
        }
    }

    /**
     * Method which is responsible for executing main query and pasring its result with its explain plan
     * @param resultSet is native result of some query
     * @param model instance of DataModel for getting important data such as tableNames, so information about result columns can be gathered
     * @return instance of ConsumedResult
     * @throws ParseException when SQLException occur during the process
     */
    @Override
    public ConsumedResult parseMainResult(ResultSet resultSet, DataModel model) throws ParseException {
        try {
            var builder = new ConsumedResult.Builder();
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                builder.addRecord();
                _consumeColumnDataToBuilder(builder, metaData, resultSet);
            }
            return builder.toResult();
        } catch (SQLException e) {
            throw new ParseException(e);
        }
    }

    /**
     * Method which is responsible for executing some query and consume it
     * @param resultSet is native result of some query
     * @return instance of ConsumedResult
     * @throws ParseException when SQLException occur during the process
     */
    @Override
    public ConsumedResult parseResultAndConsume(ResultSet resultSet) throws ParseException {
        try {
            var builder = new ConsumedResult.Builder();
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                builder.addRecord();
                _consumeColumnDataToBuilder(builder, metaData, resultSet);
            }
            return builder.toResult();
        } catch (SQLException e) {
            throw new ParseException(e);
        }
    }
}
