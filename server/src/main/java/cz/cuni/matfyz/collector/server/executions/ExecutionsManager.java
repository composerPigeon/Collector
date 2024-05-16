package cz.cuni.matfyz.collector.server.executions;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.persistor.PersistorException;
import cz.cuni.matfyz.collector.server.PersistorContainer;
import cz.cuni.matfyz.collector.server.exceptions.ErrorMessages;
import cz.cuni.matfyz.collector.server.exceptions.ExecutionManagerException;
import cz.cuni.matfyz.collector.server.exceptions.QueueExecutionsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Queue;

@Component
public class ExecutionsManager {
    @Autowired
    private PersistorContainer _persistor;

    @Autowired
    private ExecutionsQueue _queue;

    public String createExecution(String instanceName, String query) throws ExecutionManagerException {
        try {
            return _queue.createExecution(instanceName, query);
        } catch (QueueExecutionsException e) {
            String errMsg = ErrorMessages.queueInsertExecutionErrorMsg(instanceName, query);
            throw new ExecutionManagerException(errMsg, e);
        }
    }

    public ExecutionState getExecutionState(String uuid) throws ExecutionManagerException {
        try {
            ExecutionState result = _queue.getExecutionState(uuid);
            if (result == ExecutionState.NotFound) {
                result = _persistor.getExecutionState(uuid);
            }
            return result;
        } catch (QueueExecutionsException | PersistorException e) {
            String errMsg = ErrorMessages.findExecutionStateErrorMsg(uuid);
            throw new ExecutionManagerException(errMsg, e);
        }

    }

    public void setExecutionRunning(String uuid) throws ExecutionManagerException {
        try {
            _queue.setRunning(uuid);
        } catch (QueueExecutionsException e) {
            String errMsg = ErrorMessages.setExecutionRunningErrorMsg(uuid);
            throw new ExecutionManagerException(errMsg, e);
        }
    }

    public String getExecutionResult(String uuid) throws ExecutionManagerException {
        try {
            return _persistor.getExecutionResult(uuid);
        } catch (PersistorException e) {
            String errMsg = ErrorMessages.getExecutionResultErrorMsg(uuid);
            throw new ExecutionManagerException(errMsg, e);
        }

    }

    public List<Execution> getExecutionsFromQueue() throws ExecutionManagerException {
        try {
            return _queue.getExecutions();
        } catch (QueueExecutionsException e) {
            String errMsg = ErrorMessages.getExecutionsFromQueueErrorMsg();
            throw new ExecutionManagerException(errMsg, e);
        }

    }

    public void removeExecutionFromQueue(String uuid) throws ExecutionManagerException {
        try {
            _queue.removeExecution(uuid);
        } catch (QueueExecutionsException e) {
            String errMsg = ErrorMessages.removeExecutionErrorMsg(uuid);
            throw new ExecutionManagerException(errMsg, e);
        }

    }

    public void saveResult(String uuid, DataModel model) throws ExecutionManagerException {
        try {
            _persistor.saveExecutionResult(uuid, model);
            removeExecutionFromQueue(uuid);
        } catch (PersistorException e) {
            String errMsg = ErrorMessages.saveExecutionResultErrorMsg(uuid);
            throw new ExecutionManagerException(errMsg, e);
        }

    }

    public void saveError(String uuid, String errorMsg) throws ExecutionManagerException {
        try {
            _persistor.saveExecutionError(uuid, errorMsg);
            removeExecutionFromQueue(uuid);
        } catch (PersistorException e) {
            String errMsg = ErrorMessages.saveExecutionErrorErrorMsg(uuid);
            throw new ExecutionManagerException(errMsg, e);
        }
    }
}
