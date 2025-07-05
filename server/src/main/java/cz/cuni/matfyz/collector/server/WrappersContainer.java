package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.server.configurationproperties.Instance;
import cz.cuni.matfyz.collector.server.configurationproperties.SystemType;
import cz.cuni.matfyz.collector.server.configurationproperties.WrappersProperties;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.Wrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import cz.cuni.matfyz.collector.wrappers.mongodb.MongoWrapper;
import cz.cuni.matfyz.collector.wrappers.neo4j.Neo4jWrapper;
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

    private Map<String, Instance> _instances;

    @Autowired
    private WrapperInitializers _initializers;

    @PostConstruct
    public void initialize() {
        _registerInitializers();

        _instances = new HashMap<>();
        _wrappers = new HashMap<>();

        for (Instance instance : _properties.getWrappers()) {
            _instances.put(instance.getInstanceName(), instance);
        }

        _properties = null;
    }


    private void _registerInitializers() {
        _initializers.register(SystemType.MongoDB, MongoWrapper::new);
        _initializers.register(SystemType.Neo4j, Neo4jWrapper::new);
        _initializers.register(SystemType.PostgreSQL, PostgresWrapper::new);
    }

    /**
     * Method for listing all wrappers
     * @return lit of maps, where each map contain selected infos about wrapper
     */
    public List<Instance.ID> listInstances() {
        List<Instance.ID> list = new ArrayList<>();
        for (var instance : _instances.values()) {
            list.add(instance.getID());
        }
        return list;
    }



    /**
     * Method for checking if instance of instanceName exists
     * @param instanceName identifier of instance
     * @return true if instance of instanceName exist
     */
    public boolean contains(String instanceName) {
        return _instances.containsKey(instanceName);
    }

    private Wrapper _get(String instanceName) {
        if (!_wrappers.containsKey(instanceName) && _instances.containsKey(instanceName)) {
            Instance instance = _instances.get(instanceName);
            _wrappers.put(instance.getInstanceName(), _initializers.initialize(instance.getSystemType(), instance));
        }
        return _wrappers.get(instanceName);
    }

    /**
     * Method for evaluating query over specified instance
     * @param instanceName instance identifier
     * @param query query to be evaluated
     * @return DataModel instance of collected data
     * @throws WrapperException when some WrapperException occur during process
     */
    public DataModel executeQuery(String instanceName, String query) throws WrapperException {
        return _get(instanceName).executeQuery(query);
    }
}
