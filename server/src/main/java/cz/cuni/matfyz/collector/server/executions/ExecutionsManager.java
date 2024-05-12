package cz.cuni.matfyz.collector.server.executions;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.server.PersistorContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ExecutionsManager {
    @Autowired
    private PersistorContainer _persistorContainer;

    @Autowired
    private ExecutionsQueue _queue;

    public String createExecution(String instanceName, String query) {
        return _queue.createExecution(instanceName, query);
    }

    // returns model if exists or status in queue
    public String getExecutionState(String uuid) {
        String result = _persistorContainer.getPersistor().getExecution(uuid);
        if (result == null) {
            result = _queue.getExecutionStatus(uuid);
        }
        return (result == null) ? "Execution does not exists" : result;
    }

    public Execution getExecutionFromQueue() {
        return _queue.getExecution();
    }

    public void saveResult(String uuid, DataModel model) {
        _persistorContainer.getPersistor().saveExecution(uuid, model);
        _queue.removeExecution(uuid);
    }
}
