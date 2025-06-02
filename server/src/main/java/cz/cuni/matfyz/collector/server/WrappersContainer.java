package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.server.configurationproperties.Instance;
import cz.cuni.matfyz.collector.server.configurationproperties.WrappersProperties;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.Wrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import cz.cuni.matfyz.collector.wrappers.mongodb.MongoResources;
import cz.cuni.matfyz.collector.wrappers.mongodb.MongoWrapper;
import cz.cuni.matfyz.collector.wrappers.neo4j.Neo4jResources;
import cz.cuni.matfyz.collector.wrappers.neo4j.Neo4jWrapper;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresResources;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresWrapper;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Class for initializing all wrappers from properties and then provide API to use them for execution of queries
 */
@Component
public class WrappersContainer {

    @Autowired
    private WrappersProperties _properties;

    private Map<String, Wrapper> _wrappers;

    /**
     * Method for initializing all wrappers from properties
     */
    @PostConstruct
    public void init() {
        _wrappers = new HashMap<>();
        for (Instance instance : _properties.getWrappers()) {
            switch (instance.getSystemType()) {
                case PostgreSQL -> _wrappers.put(instance.getInstanceName(), new PostgresWrapper(
                        new AbstractWrapper.ConnectionData(
                                instance.getHostName(),
                                instance.getPort(),
                                PostgresResources.SYSTEM_NAME,
                                instance.getDatabaseName(),
                                instance.getCredentials().getUserName(),
                                instance.getCredentials().getPassword()
                        )
                ));
                case Neo4j -> _wrappers.put(instance.getInstanceName(), new Neo4jWrapper(
                        new AbstractWrapper.ConnectionData(
                                instance.getHostName(),
                                instance.getPort(),
                                Neo4jResources.SYSTEM_NAME,
                                instance.getDatabaseName(),
                                instance.getCredentials().getUserName(),
                                instance.getCredentials().getPassword()
                        )
                ));
                case MongoDB -> _wrappers.put(instance.getInstanceName(), new MongoWrapper(
                        new AbstractWrapper.ConnectionData(
                                instance.getHostName(),
                                instance.getPort(),
                                MongoResources.SYSTEM_NAME,
                                instance.getDatabaseName(),
                                instance.getCredentials().getUserName(),
                                instance.getCredentials().getPassword()
                        )
                ));
            }
        }
    }

    /**
     * Method for listing all wrappers
     * @return lit of maps, where each map contain selected infos about wrapper
     */
    public List<Map<String, Object>> list() {
        List<Map<String, Object>> list = new ArrayList<>();

        for (var entry : _wrappers.entrySet()) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("instanceName", entry.getKey());
            if (entry.getValue() instanceof MongoWrapper)
                map.put("type", "MongoDB");
            else if (entry.getValue() instanceof Neo4jWrapper)
                map.put("type", "Neo4j");
            else if (entry.getValue() instanceof PostgresWrapper)
                map.put("type", "PostgreSQL");
            list.add(map);
        }

        return list;
    }

    /**
     * Method for checking if instance of instanceName exists
     * @param instanceName identifier of instance
     * @return true if instance of instanceName exist
     */
    public boolean contains(String instanceName) {
        return _wrappers.containsKey(instanceName);
    }

    /**
     * Method for evaluating query over specified instance
     * @param instanceName instance identifier
     * @param query query to be evaluated
     * @return DataModel instance of collected data
     * @throws WrapperException when some WrapperException occur during process
     */
    public DataModel executeQuery(String instanceName, String query) throws WrapperException {
        return _wrappers.get(instanceName).executeQuery(query);
    }
}
