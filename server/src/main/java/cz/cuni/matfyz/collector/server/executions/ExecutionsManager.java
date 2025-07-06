package cz.cuni.matfyz.collector.server.executions;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.persistor.AbstractPersistor;
import cz.cuni.matfyz.collector.persistor.ExecutionResult;
import cz.cuni.matfyz.collector.persistor.PersistorException;
import cz.cuni.matfyz.collector.server.Initializers;
import cz.cuni.matfyz.collector.server.configurationproperties.PersistorProperties;
import cz.cuni.matfyz.collector.server.configurationproperties.SystemType;
import cz.cuni.matfyz.collector.server.exceptions.ErrorMessages;
import cz.cuni.matfyz.collector.server.exceptions.ExecutionManagerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.List;

/**
 * Class representing manager of execution processes
 */
@Component
public class ExecutionsManager implements AutoCloseable {
    private final AbstractPersistor _persistor;
    private final ExecutionsQueue _queue;
    private final ErrorMessages _errors;

    @Autowired
    public ExecutionsManager(Initializers initializers, PersistorProperties properties, ExecutionsQueue queue, ErrorMessages errorMessages) throws PersistorException {
        _persistor = initializers.initializePersistor(SystemType.MongoDB, properties);
        _queue = queue;
        _errors = errorMessages;
    }

    /**
     * Method that creates execution and insert it into ExecutionsQueue
     * @param instanceName instance identifier on query should be executed when scheduler will schedule this query execution
     * @param query query to bo execute on specified instance
     * @return generated uuid for newly created execution
     * @throws ExecutionManagerException when some QueueExecutionsException occur during process
     */
    public String createExecution(String instanceName, String query) throws ExecutionManagerException {
        try {
            return _queue.createExecution(instanceName, query);
        } catch (SQLException e) {
            String errMsg = _errors.queueInsertExecutionErrorMsg(instanceName, query, e);
            throw new ExecutionManagerException(errMsg, e);
        }
    }

    /**
     * Method getting execution state of execution
     * @param uuid execution identifier
     * @return state of execution
     * @throws ExecutionManagerException when some QueueExecutionsException or PersistorException occur during the process
     */
    public ExecutionState getExecutionState(String uuid) throws ExecutionManagerException {
        try {
            ExecutionState result = _queue.getExecutionState(uuid);
            if (result == ExecutionState.NotFound) {
                if (_persistor.containsExecution(uuid))
                    result = ExecutionState.Processed;
            }
            return result;
        } catch (SQLException | PersistorException e) {
            String errMsg = _errors.findExecutionStateErrorMsg(uuid, e);
            throw new ExecutionManagerException(errMsg, e);
        }

    }

    /**
     * Method for setting execution to running state, when query start to be process
     * @param uuid execution identifier
     * @throws ExecutionManagerException when some QueueExecutionsException occur during the process
     */
    public void setExecutionRunning(String uuid) throws ExecutionManagerException {
        try {
            _queue.setRunning(uuid);
        } catch (SQLException e) {
            String errMsg = _errors.setExecutionRunningErrorMsg(uuid, e);
            throw new ExecutionManagerException(errMsg, e);
        }
    }

    /**
     * Method for getting execution result
     * @param uuid execution identifier
     * @return json representation of DataModel or error message
     * @throws ExecutionManagerException when some PersistorException occur during the process
     */
    public ExecutionResult getExecutionResult(String uuid) throws ExecutionManagerException {
        try {
            return _persistor.getExecutionResult(uuid);
        } catch (PersistorException e) {
            String errMsg = _errors.getExecutionResultErrorMsg(uuid, e);
            throw new ExecutionManagerException(errMsg, e);
        }

    }

    /**
     * Method for fetching all waiting executions from queue
     * @return list of waiting executions to be executed by scheduler
     * @throws ExecutionManagerException when some QueueExecutionsException occur during the process
     */
    public List<Execution> getWaitingExecutionsFromQueue() throws ExecutionManagerException {
        try {
            return _queue.getWaitingExecutions();
        } catch (SQLException    e) {
            String errMsg = _errors.getExecutionsFromQueueErrorMsg(e);
            throw new ExecutionManagerException(errMsg, e);
        }

    }

    /**
     * Method for removing execution from queue. Usually after execution was executed.
     * @param uuid execution identifier
     * @throws ExecutionManagerException when some QueueExecutionsException occur during the process
     */
    private void _removeExecutionFromQueue(String uuid) throws ExecutionManagerException {
        try {
            _queue.removeExecution(uuid);
        } catch (SQLException e) {
            String errMsg = _errors.removeExecutionErrorMsg(uuid, e);
            throw new ExecutionManagerException(errMsg, e);
        }

    }

    /**
     * Method for saving result of execution into persistor
     * @param uuid execution identifier
     * @param model instance of DataModel to be saved as result
     * @throws ExecutionManagerException when some PersistorException occur during the process
     */
    public void saveResult(String uuid, DataModel model) throws ExecutionManagerException {
        try {
            _persistor.saveExecutionResult(uuid, model);
            _removeExecutionFromQueue(uuid);
        } catch (PersistorException e) {
            String errMsg = _errors.saveExecutionResultErrorMsg(uuid, e);
            throw new ExecutionManagerException(errMsg, e);
        }

    }

    /**
     * Method for saving error of execution into persistor
     * @param uuid execution identifier
     * @param errorMsg error message produced by error which interrupted process of query evaluation
     * @throws ExecutionManagerException when some PersistorException occur during the process
     */
    public void saveError(String uuid, String errorMsg) throws ExecutionManagerException {
        try {
            _persistor.saveExecutionError(uuid, errorMsg);
            _removeExecutionFromQueue(uuid);
        } catch (PersistorException e) {
            String errMsg = _errors.saveExecutionErrorErrorMsg(uuid, e);
            throw new ExecutionManagerException(errMsg, e);
        }
    }

    public void close() throws Exception {
        _persistor.close();
        _queue.close();
    }
}
