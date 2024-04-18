package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import cz.cuni.matfyz.collector.wrappers.neo4j.Neo4jWrapper;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresWrapper;

import java.util.HashMap;
import java.util.Map;

public class QueryExecutor {
    private static final QueryExecutor _instance = new QueryExecutor();
    public static QueryExecutor getInstance() { return _instance; }

    private final Map<String, AbstractWrapper> _wrappers;
    private QueryExecutor() {
        _wrappers = new HashMap<>();
        _wrappers.put(PseudoProperties.Noe4j.INSTANCE_NAME, new Neo4jWrapper(
                PseudoProperties.Noe4j.HOST,
                PseudoProperties.Noe4j.DATASET_NAME,
                PseudoProperties.Noe4j.USER,
                PseudoProperties.Noe4j.PASSWORD
        ));
        _wrappers.put(PseudoProperties.Postgres.INSTANCE_NAME, new PostgresWrapper(
                PseudoProperties.Postgres.HOST,
                PseudoProperties.Postgres.DATASET_NAME,
                PseudoProperties.Postgres.USER,
                PseudoProperties.Postgres.PASSWORD
        ));
    }

    public DataModel execute(String instanceName, String query) {
        try {
            if (_wrappers.containsKey(instanceName)) {
                return _wrappers.get(instanceName).executeQuery(query);
            }
            return null;
        } catch (WrapperException e) {
            e.printStackTrace();
            return null;
        }
    }
}
