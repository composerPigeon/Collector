package cz.cuni.matfyz.collector.wrappers.neo4j;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.summary.Plan;

public class Neo4jWrapper extends AbstractWrapper<Plan, Result> {
    private final Driver _driver;
    private final Neo4jParser _parser;
    public Neo4jWrapper(String link, String datasetName) {
        super(link, datasetName);
        _driver = GraphDatabase.driver(link, AuthTokens.basic(datasetName,"MiGWwErj5UxFfac" ));
        _parser = new Neo4jParser();
    }

    @Override
    public DataModel executeQuery(String query) throws WrapperException {
        try (
            var connection = new Neo4jConnection(_driver, _datasetName, _parser)
        ){
            var dataModel = new DataModel(query, Neo4jResources.DATABASE_NAME, _datasetName);
            var result = connection.executeMainQuery(query, dataModel);

            var collector = new Neo4jDataCollector(connection, dataModel, _datasetName);
            return collector.collectData(result);
        } catch (QueryExecutionException e) {
            throw new WrapperException(e);
        }
    }
}
