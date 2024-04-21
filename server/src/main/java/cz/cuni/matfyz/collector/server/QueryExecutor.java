package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import cz.cuni.matfyz.collector.wrappers.mongodb.MongoWrapper;
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
        for (PseudoProperties.DbInstance instance : PseudoProperties.getDbInstances())  {
            _addWrapper(instance.dbType(), instance);
        }
    }

    private void _addWrapper(PseudoProperties.DbType type, PseudoProperties.DbInstance instance) {
        switch (type) {
            case Neo4j -> _wrappers.put(instance.instanceName(), new Neo4jWrapper(
                    instance.hostName(),
                    instance.datasetName(),
                    instance.userName(),
                    instance.password()
            ));
            case PostgreSQL -> _wrappers.put(instance.instanceName(), new PostgresWrapper(
                    instance.hostName(),
                    instance.datasetName(),
                    instance.userName(),
                    instance.password()
            ));
            case MongoDB -> _wrappers.put(instance.instanceName(), new MongoWrapper(
                    instance.hostName(),
                    instance.datasetName(),
                    instance.userName(),
                    instance.password()
            ));
        }
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
