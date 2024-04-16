package cz.cuni.matfyz.collector.wrappers.neo4j;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractParser;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.*;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.util.Pair;

public class Neo4jParser extends AbstractParser<Plan, Result> {

    private void _parseTableName(DataModel model, Plan operator) {
        String details = operator.arguments().get("Details").asString();
        String tableName = details.split(":")[1];
        model.toDatasetData().addTable(tableName);
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

        model.toDatasetData().addIndex(indexType + ':' + indexIdentifiers[0] + ':' + indexIdentifiers[1]);
    }

    public void _parseOperator(DataModel model, Plan operator) {
        if (operator.operatorType().contains("NodeByLabelScan")) {
            _parseTableName(model, operator);
        } else if (operator.operatorType().contains("Index")) {
            _parseIndexName(model, operator);
        }

        for (Plan child : operator.children()) {
            _parseOperator(model, child);
        }
    }

    @Override
    public void parseExplainTree(DataModel model, Plan explainTree) throws ParseException {
        _parseOperator(model, explainTree);
    }

    private Object _parseToObject(Value value) throws ParseException {
        if (value instanceof IntegerValue intVal)
            return intVal.asInt();
        else if (value instanceof FloatValue floatValue)
            return floatValue.asDouble();
        else if (value instanceof BooleanValue booleanValue)
            return booleanValue.asBoolean();
        else if (value instanceof StringValue stringValue)
            return stringValue.asString();
        else if (value instanceof ListValue listValue)
            return listValue.asList();
        else if (value instanceof DateValue dateValue)
            return dateValue.asLocalDate();
        else if (value instanceof DateTimeValue dateTimeValue)
            return dateTimeValue.asZonedDateTime();
        else if (value instanceof NullValue)
            return null;
        else
            throw new ParseException("ValueType" + value.toString() + "needs to be parsed");
    }

    private Set<Map.Entry<String, Object>> _parseNodeToMap(Node node) throws ParseException {
        var map = new HashMap<String, Object>();
        for (String colName : node.keys()) {
            Object value = _parseToObject(node.get(colName));
            if (value != null)
                map.put(colName, value);
        }
        return map.entrySet();
    }

    private Set<Map.Entry<String, Object>> _parseRelationToMap(Relationship relation) throws ParseException {
        var map = new HashMap<String, Object>();
        for (String colName : relation.keys()) {
            Object value = _parseToObject(relation.get(colName));
            if (value != null)
                map.put(colName, value);
        }
        return map.entrySet();
    }


    private void _addDataToBuilder(CachedResult.Builder builder, Record record, boolean addSize) throws ParseException {
        for (Pair<String, Value> pair : record.fields()) {
            if (pair.value() instanceof NodeValue nodeValue) {
                for (Map.Entry<String, Object> entry : _parseNodeToMap(nodeValue.asNode())) {
                    builder.toLastRecordAddValue(entry.getKey(), entry.getValue());
                    if (addSize)
                        builder.addSize(Neo4jResources.DefaultSizes.getAvgColumnSize(entry.getValue()));
                }
            }
            else if (pair.value() instanceof RelationshipValue relationshipValue) {
                for (Map.Entry<String, Object> entry : _parseRelationToMap(relationshipValue.asRelationship())) {
                    builder.toLastRecordAddValue(entry.getKey(), entry.getValue());
                    if (addSize)
                        builder.addSize(Neo4jResources.DefaultSizes.getAvgColumnSize(entry.getValue()));
                }
            }
            else {
                Object value = _parseToObject(pair.value());
                if (value != null)
                    builder.toLastRecordAddValue(pair.key(), value);
                if (addSize)
                    builder.addSize(Neo4jResources.DefaultSizes.getAvgColumnSize(value));
            }
        }
    }

    @Override
    public CachedResult parseResult(Result result) throws ParseException {
        var builder = new CachedResult.Builder();
        while (result.hasNext()) {
            var record = result.next();
            builder.addEmptyRecord();
            _addDataToBuilder(builder, record, false);
        }
        return builder.toResult();
    }

    @Override
    public CachedResult parseMainResult(Result result, DataModel withModel) throws ParseException {
        var builder = new CachedResult.Builder();
        while (result.hasNext()) {
            var record = result.next();
            builder.addEmptyRecord();
            _addDataToBuilder(builder, record, true);
        }
        return builder.toResult();
    }
}
