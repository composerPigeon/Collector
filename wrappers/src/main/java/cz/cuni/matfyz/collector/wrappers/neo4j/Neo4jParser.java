package cz.cuni.matfyz.collector.wrappers.neo4j;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractParser;
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

public class Neo4jParser extends AbstractParser<ResultSummary, Result> {

    private void _parseExecutionTime(DataModel model, ResultSummary summary ) {
        long nanoseconds = summary.resultAvailableAfter(TimeUnit.NANOSECONDS);
        model.resultData().setExecutionTime((double) nanoseconds / (1_000_000));
    }
    private void _parseNodeTableName(DataModel model, Plan operator) {
        String details = operator.arguments().get("Details").asString();
        String tableName = details.split(":")[1];
        model.datasetData().addTable(tableName);
    }

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
    private void _parseRelationTableName(DataModel model, Plan operator) {
        String details = operator.arguments().get("Details").asString();
        String tableName = _parseRelationDetailsForLabel(details);
        model.datasetData().addTable(tableName);
    }

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

    private void _parseIndexName(DataModel model, Plan operator) {
        String[] details = operator.arguments().get("Details").asString().split(" ");
        String indexType = details[0];
        String[] indexIdentifiers = _parseIndexIdentifier(details[2].split(":")[1]);

        model.datasetData().addIndex(indexType + ':' + indexIdentifiers[0] + ':' + indexIdentifiers[1]);
    }

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

    @Override
    public void parseExplainTree(DataModel model, ResultSummary summary) throws ParseException {
        _parseExecutionTime(model, summary);
        _parseOperator(model, summary.plan());
    }

    // Parse Result
    private Set<Map.Entry<String, PropertyData>> _parseNodeToMap(Node node) throws ParseException {
        var map = new HashMap<String, PropertyData>();
        for (String colName : node.keys()) {
            PropertyData data = PropertyData.fromValue(node.get(colName));
            if (data != null)
                map.put(colName, data);
        }
        return map.entrySet();
    }

    private Set<Map.Entry<String, PropertyData>> _parseRelationToMap(Relationship relation) throws ParseException {
        var map = new HashMap<String, PropertyData>();
        for (String colName : relation.keys()) {
            PropertyData data = PropertyData.fromValue(relation.get(colName));
            if (data != null)
                map.put(colName, data);
        }
        return map.entrySet();
    }


    private void _addDataToBuilder(CachedResult.Builder builder, Record record, boolean addSize, boolean collectColumnData) throws ParseException {
        for (Pair<String, Value> pair : record.fields()) {
            if (pair.value() instanceof NodeValue nodeValue) {
                for (Map.Entry<String, PropertyData> entry : _parseNodeToMap(nodeValue.asNode())) {
                    PropertyData propData = entry.getValue();
                    builder.toLastRecordAddValue(entry.getKey(), propData.getValue());
                    if (addSize)
                        builder.addSize(Neo4jResources.DefaultSizes.getAvgColumnSizeByType(propData.getType()));
                    if (collectColumnData)
                        builder.addColumnType(entry.getKey(), propData.getType());
                }
            }
            else if (pair.value() instanceof RelationshipValue relationshipValue) {
                for (Map.Entry<String, PropertyData> entry : _parseRelationToMap(relationshipValue.asRelationship())) {
                    PropertyData propData = entry.getValue();
                    builder.toLastRecordAddValue(entry.getKey(), propData.getValue());
                    if (addSize)
                        builder.addSize(Neo4jResources.DefaultSizes.getAvgColumnSizeByType(propData.getType()));
                    if (collectColumnData)
                        builder.addColumnType(entry.getKey(), propData.getType());
                }
            }
            else {
                PropertyData propData = PropertyData.fromValue(pair.value());
                if (propData != null) {
                    builder.toLastRecordAddValue(pair.key(), propData.getValue());
                    if (addSize)
                        builder.addSize(Neo4jResources.DefaultSizes.getAvgColumnSizeByType(propData.getType()));
                    if (collectColumnData)
                        builder.addColumnType(pair.key(), propData.getType());
                }
            }
        }
    }

    @Override
    public CachedResult parseResult(Result result) throws ParseException {
        var builder = new CachedResult.Builder();
        while (result.hasNext()) {
            var record = result.next();
            builder.addEmptyRecord();
            _addDataToBuilder(builder, record, false, false);
        }
        return builder.toResult();
    }

    @Override
    public CachedResult parseMainResult(Result result, DataModel withModel) throws ParseException {
        var builder = new CachedResult.Builder();
        while (result.hasNext()) {
            var record = result.next();
            builder.addEmptyRecord();
            _addDataToBuilder(builder, record, true, true);
        }
        return builder.toResult();
    }

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
