package cz.cuni.matfyz.collector.wrappers.neo4j.components;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.AbstractExplainPlanParser;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperExceptionsFactory;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ResultSummary;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Neo4jExplainPlanParser extends AbstractExplainPlanParser<ResultSummary> {

    public Neo4jExplainPlanParser(WrapperExceptionsFactory exceptionsFactory) {
        super(exceptionsFactory);
    }

    /**
     * Method which saves execution time from explain to data model
     * @param model DataModel to save parsed data
     * @param summary part of explain result
     */
    private void _parseExecutionTime(DataModel model, ResultSummary summary ) {
        long nanoseconds = summary.resultAvailableAfter(TimeUnit.NANOSECONDS);
        model.setResultExecutionTime((double) nanoseconds / (1_000_000));
    }

    /**
     * Method for getting all used labels by main query
     * @param model DataModel to save parsed data
     * @param operator represents one node of explain tree
     */
    private void _parseNodeTableName(DataModel model, Plan operator) {
        String details = operator.arguments().get("Details").asString();
        String tableName = details.split(":")[1];
        model.addKind(tableName);
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
        model.addKind(tableName);
    }

    /**
     * Method parsing index identifier to tokens
     * @param identifier index identifier created from information such as label, property name and type
     * @return string array of tokens from index
     */
    private void _parseIndexIdentifier(String identifier, IndexParseRecord.Builder builder) {
        StringBuilder buffer = new StringBuilder();
        for (char ch : identifier.toCharArray()) {
            if (ch == '(') {
                builder.setLabel(buffer.toString());
                buffer.setLength(0);
            } else if (ch == ',') {
                builder.addProperty(buffer.toString());
                buffer.setLength(0);
            } else if (ch == ')') {
                break;
            } else {
                buffer.append(ch);
            }
        }
    }

    /**
     * Method for getting index identifier from explain relevant to query
     * @param model DataModel to save data
     * @param operator explain tree node
     */
    private void _parseIndexName(DataModel model, Plan operator) {
        IndexParseRecord.Builder inxBuilder = new IndexParseRecord.Builder();
        String[] details = operator.arguments().get("Details").asString().split(" ");
        inxBuilder.setIndexType(details[0]);
        _parseIndexIdentifier(details[2].split(":")[1], inxBuilder);

        model.addIndex(inxBuilder.build().getIndexName());
    }

    /**
     * Method for parsing types of different Neo4j operators
     * @param model dataModel to save results
     * @param operator actual explain tree node to be parsed
     */
    private void _parseOperator(DataModel model, Plan operator) {
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
     * @param plan explain tree to be parsed
     * @throws ParseException is there to implement abstract method
     */
    @Override
    public void parsePlan(ResultSummary plan, DataModel model) throws ParseException {
        _parseExecutionTime(model, plan);
        _parseOperator(model, plan.profile());
    }
}
