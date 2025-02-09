package cz.cuni.matfyz.collector.wrappers.neo4j;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.*;
import cz.cuni.matfyz.collector.wrappers.cachedresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;

import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ResultSummary;

/**
 * Class which is responsible to connect to neo4j and enable query execution
 */
public class Neo4jConnection extends AbstractConnection<ResultSummary, Result, String> {
    private final Session _session;
    public Neo4jConnection(Driver neo4jDriver, String datasetName, Neo4jParser parser) {
        super(parser);
        _session = neo4jDriver.session(
                SessionConfig.builder().withDatabase(datasetName).build()
        );
    }

    /**
     * Mathod which executes main query, parse its result and also its explain plan
     * @param query inputed query
     * @param toModel DataModel which is used for storing data parsed from explain tree
     * @return instance of ConsumedResult
     * @throws QueryExecutionException when some Neo4jException or ParseException occur during process
     */
    @Override
    public ConsumedResult executeMainQuery(String query, DataModel toModel) throws QueryExecutionException {
        try {
            Result result = _session.run(Neo4jResources.getExplainPlanQuery(query));
            var consumedResult = _parser.parseMainResult(result, toModel);
            _parser.parseExplainTree(toModel, result.consume());
            return consumedResult;
        } catch (Neo4jException | ParseException e) {
            throw new QueryExecutionException(e);
        }
    }

    /**
     * Method which will consume result of inputted query and return it as an instance of ConsumedResult
     * @param query inputted query
     * @return instance of ConsumedResult
     * @throws QueryExecutionException when some Neo4jException or ParseException occur during process
     */
    @Override
    public ConsumedResult executeQueryAndConsume(String query) throws QueryExecutionException {
        try {
            Result result = _session.run(query);
            return _parser.parseResultAndConsume(result);
        } catch (Neo4jException | ParseException e) {
            throw new QueryExecutionException(e);
        }
    }

    /**
     * Method which is responsible for executing query and caching and parsing result to CachedResult
     * @param query inputted query
     * @return instance of CachedResult
     * @throws QueryExecutionException when some Neo4jException or ParseException occur during process
     */
    @Override
    public CachedResult executeQuery(String query) throws QueryExecutionException {
        try {
            Result result = _session.run(query);
            return _parser.parseResult(result);
        } catch (Neo4jException | ParseException e) {
            throw new QueryExecutionException(e);
        }
    }

    /**
     * Method which implements interface AutoClosable and closes all resources after query evaluation is ended
     */
    @Override
    public void close() {
        _session.close();
    }
}
