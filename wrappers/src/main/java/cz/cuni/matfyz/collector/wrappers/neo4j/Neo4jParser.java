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

    public void _parseOperator(DataModel model, Plan operator) {
        if (operator.operatorType().contains("NodeByLabelScan")) {
            _parseTableName(model, operator);
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
        else if (value instanceof StringValue stringValue)
            return stringValue.asString();
        else
            throw new ParseException("Invalid Value from Neo4j Result");
    }

    private Set<Map.Entry<String, Object>> _parseNodeToMap(Node node) throws ParseException {
        var map = new HashMap<String, Object>();
        for (String colName : node.keys()) {
            map.put(colName, _parseToObject(node.get(colName)));
        }
        return map.entrySet();
    }

    private Set<Map.Entry<String, Object>> _parseRelationToMap(Relationship relation) throws ParseException {
        var map = new HashMap<String, Object>();
        for (String colName : relation.keys()) {
            map.put(colName, _parseToObject(relation.get(colName)));
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
