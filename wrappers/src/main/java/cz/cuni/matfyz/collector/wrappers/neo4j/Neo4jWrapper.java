package cz.cuni.matfyz.collector.wrappers.neo4j;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.AbstractExplainPlanParser;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.AbstractQueryResultParser;
import cz.cuni.matfyz.collector.wrappers.exceptions.ConnectionException;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.neo4j.components.Neo4jConnection;
import cz.cuni.matfyz.collector.wrappers.neo4j.components.Neo4jDataCollector;
import cz.cuni.matfyz.collector.wrappers.neo4j.components.Neo4jExplainPlanParser;
import cz.cuni.matfyz.collector.wrappers.neo4j.components.Neo4jQueryResultParser;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.summary.ResultSummary;

/**
 * Class which represents wrapper that is connected to Neo4j database and evaluate queries over it
 */
public class Neo4jWrapper extends AbstractWrapper<Result, String, ResultSummary> {
    private final Driver _driver;
    public Neo4jWrapper(ConnectionData connectionData) {
        super(connectionData);
        _driver = GraphDatabase.driver(
                Neo4jResources.getConnectionLink(connectionData.host(), connectionData.port(), connectionData.databaseName()),
                AuthTokens.basic(connectionData.user(), connectionData.password())
        );
    }

    @Override
    protected AbstractQueryResultParser<Result> createResultParser() {
        return new Neo4jQueryResultParser(getExceptionsFactory());
    }

    @Override
    protected AbstractExplainPlanParser<ResultSummary> createExplainPlanParser() {
        return new Neo4jExplainPlanParser(getExceptionsFactory());
    }

    @Override
    protected Neo4jConnection createConnection(ExecutionContext<Result, String, ResultSummary> context) throws ConnectionException {
        return new Neo4jConnection(_driver, _connectionData.databaseName(), getExceptionsFactory());
    }

    @Override
    protected String parseInputQuery(ExecutionContext<Result, String, ResultSummary> context) {
        return context.getInputQuery();
    }

    @Override
    protected Neo4jDataCollector createDataCollector(ExecutionContext<Result, String, ResultSummary> context) throws DataCollectException {
        try {
            return new Neo4jDataCollector(context, _resultParser);
        } catch (ConnectionException e) {
            throw getExceptionsFactory().dataCollectorNotInitialized(e);
        }
    }

    @Override
    public void close() {
        _driver.close();
    }
}
