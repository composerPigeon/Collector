package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.persistor.AbstractPersistor;
import cz.cuni.matfyz.collector.persistor.ExecutionResult;
import cz.cuni.matfyz.collector.persistor.MongoPersistor;
import cz.cuni.matfyz.collector.persistor.PersistorException;
import cz.cuni.matfyz.collector.server.configurationproperties.PersistorProperties;
import cz.cuni.matfyz.collector.server.executions.ExecutionState;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Class which is responsible for loading properties and initialize peristor and then provide API for accessing it.
 */
@Component
public class PersistorContainer {

    @Autowired
    private PersistorProperties _properties;

    private AbstractPersistor _persistor;

    @PostConstruct
    public void init() {
        _persistor = new MongoPersistor(
                _properties.getHostName(),
                _properties.getPort(),
                _properties.getDatabaseName(),
                _properties.getCredentials().userName(),
                _properties.getCredentials().password()
        );
    }

    /**
     * Method for saving execution result into persistor
     * @param uuid execution identifier
     * @param model instance of DataModel with collected stats
     * @throws PersistorException when PersistorException occur in process
     */
    public void saveExecutionResult(String uuid, DataModel model) throws PersistorException {
        _persistor.saveExecutionResult(uuid, model);
    }

    /**
     * Method for saving execution error into persistor
     * @param uuid execution identifier
     * @param errorMsg error message to be saved
     * @throws PersistorException when PersistorException occur in process
     */
    public void saveExecutionError(String uuid, String errorMsg) throws PersistorException {
        _persistor.saveExecutionError(uuid, errorMsg);
    }

    /**
     * Method for getting result of execution
     * @param uuid execution identifier
     * @return json of DataModel or error message if some error during evaluation occurred
     * @throws PersistorException when PersistorException occur in process
     */
    public ExecutionResult getExecutionResult(String uuid)  throws PersistorException {
        return _persistor.getExecutionResult(uuid);
    }

    /**
     * Method for getting execution state
     * @param uuid execution identifier
     * @return execution state
     * @throws PersistorException when PersistorException occur in process
     */
    public ExecutionState getExecutionState(String uuid) throws PersistorException {
        if (_persistor.containsExecution(uuid))
            return ExecutionState.Processed;
        else
            return ExecutionState.NotFound;
    }
}
