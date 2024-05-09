package cz.cuni.matfyz.collector.wrappers.neo4j;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.*;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;

import org.neo4j.driver.*;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ResultSummary;

public class Neo4jConnection extends AbstractConnection<ResultSummary, Result, String> {
    private final Session _session;
    public Neo4jConnection(Driver neo4jDriver, String datasetName, Neo4jParser parser) {
        super(parser);
        _session = neo4jDriver.session(
                SessionConfig.builder().withDatabase(datasetName).build()
        );
    }

    @Override
    public CachedResult executeMainQuery(String query, DataModel toModel) throws QueryExecutionException {
        try {
            Result result = _session.run(Neo4jResources.getExplainPlanQuery(query));
            var cachedResult = _parser.parseMainResult(result, toModel);
            _parser.parseExplainTree(toModel, result.consume());
            return cachedResult;
        } catch (ParseException e) {
            throw new QueryExecutionException(e);
        }
    }

    @Override
    public CachedResult executeQuery(String query) throws QueryExecutionException {
        try {
            Result result = _session.run(query);
            return _parser.parseResult(result);
        } catch (ParseException e) {
            throw new QueryExecutionException(e);
        }
    }

    @Override
    public void close() {
        _session.close();
    }
}
