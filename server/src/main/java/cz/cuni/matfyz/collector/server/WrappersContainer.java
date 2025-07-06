package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.server.configurationproperties.Instance;
import cz.cuni.matfyz.collector.server.configurationproperties.WrappersProperties;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.Wrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Class for initializing all wrappers from properties and then provide API to use them for execution of queries
 */
@Component
public class WrappersContainer implements AutoCloseable {

    private final Map<String, Wrapper> _wrappers;

    private final WrappersProperties _properties;

    private final Initializers _initializers;

    @Autowired
    public WrappersContainer(Initializers initializers, WrappersProperties properties) {
        _initializers = initializers;
        _properties = properties;
        _wrappers = new HashMap<>();
    }

    /**
     * Method for listing all wrappers
     * @return lit of maps, where each map contain selected infos about wrapper
     */
    public List<Instance.ID> listInstances() {
        List<Instance.ID> list = new ArrayList<>();
        for (var instance : _properties.getInstances()) {
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
        if (_wrappers.containsKey(instanceName))
            return true;
        return _properties.contains(instanceName);
    }

    private Wrapper _get(String instanceName) throws WrapperException {
        if (!_wrappers.containsKey(instanceName) && _properties.contains(instanceName)) {
            Instance instance = _properties.getByName(instanceName);
            _wrappers.put(instance.getInstanceName(), _initializers.initializeWrapper(instance.getSystemType(), instance));
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

    public void close() throws WrapperException {
        for (var wrapper : _wrappers.values()) {
            wrapper.close();
        }
    }
}
