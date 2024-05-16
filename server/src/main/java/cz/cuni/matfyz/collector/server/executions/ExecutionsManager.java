package cz.cuni.matfyz.collector.server.executions;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.server.PersistorContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ExecutionsManager {
    @Autowired
    private PersistorContainer _persistor;

    @Autowired
    private ExecutionsQueue _queue;

    public String createExecution(String instanceName, String query) {
        return _queue.createExecution(instanceName, query);
    }

    public ExecutionState getExecutionState(String uuid) {
        ExecutionState result = _queue.getExecutionState(uuid);
        if (result == ExecutionState.NotFound) {
            result = _persistor.getExecutionState(uuid);
        }
        return result;
    }

    public void setExecutionRunning(String uuid) {
        _queue.setRunning(uuid);
    }

    public String getExecutionResult(String uuid) {
        return _persistor.getExecutionResult(uuid);
    }

    public Execution getExecutionFromQueue() {
        return _queue.getExecution();
    }

    public void saveResult(String uuid, DataModel model) {
        _persistor.saveExecutionResult(uuid, model);
        _queue.removeExecution(uuid);
    }
}
