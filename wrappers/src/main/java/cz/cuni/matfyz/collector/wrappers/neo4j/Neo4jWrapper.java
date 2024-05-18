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

/**
 * Class which represents wrapper that is connected to Neo4j database and evaluate queries over it
 */
public class Neo4jWrapper extends AbstractWrapper {
    private final Driver _driver;
    private final Neo4jParser _parser;
    public Neo4jWrapper(String host, int port, String datasetName, String userName, String password) {
        super(host, port, datasetName, userName, password);
        _driver = GraphDatabase.driver(Neo4jResources.getConnectionLink(host, port, datasetName), AuthTokens.basic(userName, password));
        _parser = new Neo4jParser();
    }

    /**
     * Method which executes the query and collects all statistics which are return as DataModel instance
     * @param query inputted query
     * @return instance of DataModel
     * @throws WrapperException when some of implementing exceptions occur during the evaluation process
     */
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
