package cz.cuni.matfyz.collector.wrappers.neo4j;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractParser;
import cz.cuni.matfyz.collector.wrappers.cachedresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.*;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.util.Pair;

/**
 * Class responsible for parsing neo4j results and explain plan
 */
public class Neo4jParser extends AbstractParser<ResultSummary, Result> {

    /**
     * Method which saves execution time from explain to data model
     * @param model DataModel to save parsed data
     * @param summary part of explain result
     */
    private void _parseExecutionTime(DataModel model, ResultSummary summary ) {
        long nanoseconds = summary.resultAvailableAfter(TimeUnit.NANOSECONDS);
        model.resultData().setExecutionTime((double) nanoseconds / (1_000_000));
    }

    /**
     * Method for getting all used labels by main query
     * @param model DataModel to save parsed data
     * @param operator represents one node of explain tree
     */
    private void _parseNodeTableName(DataModel model, Plan operator) {
        String details = operator.arguments().get("Details").asString();
        String tableName = details.split(":")[1];
        model.datasetData().addTable(tableName);
    }

    /**
     * Method for parsing details to get edges label
     * @param details to be parsed
     * @return name of label as string
     */
    private String _parseRelationDetailsForLabel(String details) {
        StringBuilder buffer = new StringBuilder();
        Boolean isInEdge = null;
        for (char ch : details.toCharArray()) {
            if (isInEdge == null) {
                if (ch == '[')
                    isInEdge = false;
            } else if (!isInEdge){
                if (ch == ':')
                    isInEdge = true;
            } else {
                if (ch == ']')
                    break;
                else
                    buffer.append(ch);
            }
        }
        return buffer.toString();
    }

    /**
     * Method for parsing edge labels used by query
     * @param model to save labels
     * @param operator node of explain tree
     */
    private void _parseRelationTableName(DataModel model, Plan operator) {
        String details = operator.arguments().get("Details").asString();
        String tableName = _parseRelationDetailsForLabel(details);
        model.datasetData().addTable(tableName);
    }

    /**
     * Method parsing index identifier to tokens
     * @param identifier index identifier created from information such as label, property name and type
     * @return string array of tokens from index
     */
    private String[] _parseIndexIdentifier(String identifier) {
        StringBuilder label = new StringBuilder();
        StringBuilder prop = new StringBuilder();
        boolean afterParenthesis = false;
        for (char ch : identifier.toCharArray()) {
            if (ch == '(') {
                afterParenthesis = true;
            } else if (afterParenthesis) {
                if (ch == ')') {
                    break;
                } else {
                    prop.append(ch);
                }
            } else {
                label.append(ch);
            }
        }

        return new String[] { label.toString(), prop.toString() };
    }

    /**
     * Mathod for getting index identifier from explain relevant to query
     * @param model DataModel to save data
     * @param operator explain tree node
     */
    private void _parseIndexName(DataModel model, Plan operator) {
        String[] details = operator.arguments().get("Details").asString().split(" ");
        String indexType = details[0];
        String[] indexIdentifiers = _parseIndexIdentifier(details[2].split(":")[1]);

        model.datasetData().addIndex(indexType + ':' + indexIdentifiers[0] + ':' + indexIdentifiers[1]);
    }

    /**
     * Method for parsing types of different Neo4j operators
     * @param model dataModel to save results
     * @param operator actual explain tree node to be parsed
     */
    public void _parseOperator(DataModel model, Plan operator) {
        if (operator.operatorType().contains("NodeByLabel")) {
            _parseNodeTableName(model, operator);
        } else if (operator.operatorType().contains("RelationshipType")) {
            _parseRelationTableName(model, operator);
        } else if (operator.operatorType().contains("Index")) {
            _parseIndexName(model, operator);
        }

        for (Plan child : operator.children()) {
            _parseOperator(model, child);
        }
    }

    /**
     * Method for parsing explain for important information
     * @param model instance of DataModel where collected information are stored
     * @param summary explain tree to be parsed
     * @throws ParseException is there to implement abstract method
     */
    @Override
    public void parseExplainTree(DataModel model, ResultSummary summary) throws ParseException {
        _parseExecutionTime(model, summary);
        _parseOperator(model, summary.plan());
    }

    // Parse Result

    /**
     * Method which parse node to map
     * @param node node to be parsed
     * @return parsed map
     */
    private Set<Map.Entry<String, PropertyData>> _parseNodeToMap(Node node) {
        var map = new HashMap<String, PropertyData>();
        for (String colName : node.keys()) {
            PropertyData data = PropertyData.fromValue(node.get(colName));
            if (data != null)
                map.put(colName, data);
        }
        return map.entrySet();
    }

    /**
     * Method which parse egde to map
     * @param relation edge to be parsed
     * @return parsed map
     */
    private Set<Map.Entry<String, PropertyData>> _parseRelationToMap(Relationship relation) {
        var map = new HashMap<String, PropertyData>();
        for (String colName : relation.keys()) {
            PropertyData data = PropertyData.fromValue(relation.get(colName));
            if (data != null)
                map.put(colName, data);
        }
        return map.entrySet();
    }


    /**
     * Method responsible for adding record from native result to CachedResult
     * @param builder builder to add records and then build CachedResult
     * @param record is native record from result
     */
    private void _addDataToBuilder(CachedResult.Builder builder, Record record) {
        for (Pair<String, Value> pair : record.fields()) {
            if (pair.value() instanceof NodeValue nodeValue) {
                for (Map.Entry<String, PropertyData> entry : _parseNodeToMap(nodeValue.asNode())) {
                    PropertyData propData = entry.getValue();
                    builder.toLastRecordAddValue(entry.getKey(), propData.getValue());
                }
            }
            else if (pair.value() instanceof RelationshipValue relationshipValue) {
                for (Map.Entry<String, PropertyData> entry : _parseRelationToMap(relationshipValue.asRelationship())) {
                    PropertyData propData = entry.getValue();
                    builder.toLastRecordAddValue(entry.getKey(), propData.getValue());
                }
            }
            else {
                PropertyData propData = PropertyData.fromValue(pair.value());
                if (propData != null) {
                    builder.toLastRecordAddValue(pair.key(), propData.getValue());
                }
            }
        }
    }

    /**
     * Method for parsing ordinal result to Cached result
     * @param result result of some query
     * @return insance of CachedResult
     */
    @Override
    public CachedResult parseResult(Result result) {
        var builder = new CachedResult.Builder();
        while (result.hasNext()) {
            var record = result.next();
            builder.addEmptyRecord();
            _addDataToBuilder(builder, record);
        }
        return builder.toResult();
    }

    // Parse Main Result

    /**
     * Method which will incrementally compute statistics for result record by record
     * @param builder builder which consumes the data from result
     * @param record native record from result
     */
    private void _consumeDataToBuilder(ConsumedResult.Builder builder, Record record) {
        for (Pair<String, Value> pair : record.fields()) {
            if (pair.value() instanceof NodeValue nodeValue) {
                for (Map.Entry<String, PropertyData> entry : _parseNodeToMap(nodeValue.asNode())) {
                    PropertyData propData = entry.getValue();
                    builder.addByteSize(Neo4jResources.DefaultSizes.getAvgColumnSizeByType(propData.getType()));
                    builder.addColumnType(entry.getKey(), propData.getType());
                }
            }
            else if (pair.value() instanceof RelationshipValue relationshipValue) {
                for (Map.Entry<String, PropertyData> entry : _parseRelationToMap(relationshipValue.asRelationship())) {
                    PropertyData propData = entry.getValue();
                    builder.addByteSize(Neo4jResources.DefaultSizes.getAvgColumnSizeByType(propData.getType()));
                    builder.addColumnType(entry.getKey(), propData.getType());
                }
            }
            else {
                PropertyData propData = PropertyData.fromValue(pair.value());
                if (propData != null) {
                    builder.addByteSize(Neo4jResources.DefaultSizes.getAvgColumnSizeByType(propData.getType()));
                    builder.addColumnType(pair.key(), propData.getType());
                }
            }
        }
    }

    /**
     * Method which parses native result of main query ti consumed one
     * @param result is native result of some query
     * @param withModel instance of DataModel for getting important data such as tableNames, so information about result columns can be gathered
     * @return instance of ConsumedResult
     */
    @Override
    public ConsumedResult parseMainResult(Result result, DataModel withModel) {
        var builder = new ConsumedResult.Builder();
        while (result.hasNext()) {
            var record = result.next();
            builder.addRecord();
            _consumeDataToBuilder(builder, record);
        }
        return builder.toResult();
    }

    /**
     * Mathod which will take result of some ordinal query and consume it
     * @param result is native result of some query
     * @return instance of ConsumedResult
     */
    @Override
    public ConsumedResult parseResultAndConsume(Result result) {
        var builder = new ConsumedResult.Builder();
        while(result.hasNext()) {
            var record = result.next();
            builder.addRecord();
            _consumeDataToBuilder(builder, record);
        }
        return builder.toResult();
    }

    /**
     * Class which represents properties of entities from neo4j graph
     */
    private static class PropertyData {
        private Object _value;
        private String _type;

        private PropertyData(Object value, String type) {
            _value = value;
            _type = type;
        }

        public Object getValue() {
            return _value;
        }
        public String getType() {
            return _type;
        }

        /**
         * Static method which will parse value of Value type to instance of this class
         * @param value value gathered from native result
         * @return instance of newly created PropertyData
         */
        public static PropertyData fromValue(Value value) {
            if (value.isNull())
                return null;
            else if (value instanceof BooleanValue boolValue)
                return new PropertyData(boolValue.asBoolean(), "Boolean");
            else if (value instanceof DateValue dateValue)
                return new PropertyData(dateValue.asLocalDate(), "Date");
            else if (value instanceof DurationValue durValue)
                return new PropertyData(durValue.asIsoDuration(), "Duration");
            else if (value instanceof FloatValue floatValue)
                return new PropertyData(floatValue.asDouble(), "Float");
            else if (value instanceof IntegerValue intValue)
                return new PropertyData(intValue.asLong(), "Integer");
            else if (value instanceof ListValue listValue)
                return new PropertyData(listValue.asList(), "List");
            else if (value instanceof LocalDateTimeValue localDateTimeValue)
                return new PropertyData(localDateTimeValue.asLocalDateTime(), "LocalDateTime");
            else if (value instanceof LocalTimeValue localTimeValue)
                return new PropertyData(localTimeValue.asLocalTime(), "LocalTime");
            else if (value instanceof PointValue pointValue)
                return new PropertyData(pointValue.asPoint(), "Point");
            else if (value instanceof StringValue strValue)
                return new PropertyData(strValue.asString(), "String");
            else if (value instanceof DateTimeValue dateTimeValue)
                return new PropertyData(dateTimeValue.asZonedDateTime(), "ZonedDateTime");
            else if (value instanceof TimeValue timeValue)
                return new PropertyData(timeValue.asOffsetTime(), "ZonedTime");
            else
                throw new ClassCastException("Neo4j Value " + value.toString() + "cannot by parsed to object");
        }
    }
}
