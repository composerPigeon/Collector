package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.persistor.AbstractPersistor;
import cz.cuni.matfyz.collector.persistor.MongoPersistor;
import cz.cuni.matfyz.collector.server.configurationproperties.PersistorProperties;
import cz.cuni.matfyz.collector.server.executions.Execution;
import cz.cuni.matfyz.collector.server.executions.ExecutionState;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
                _properties.getDatasetName(),
                _properties.getCredentials().getUserName(),
                _properties.getCredentials().getPassword()
        );
    }

    public void saveExecutionResult(String instanceName, DataModel model) {
        _persistor.saveExecution(instanceName, model);
    }

    public String getExecutionResult(String uuid) {
        return _persistor.getExecutionResult(uuid);
    }

    public ExecutionState getExecutionState(String uuid) {
        if (_persistor.getExecutionStatus(uuid))
            return ExecutionState.Processed;
        else
            return ExecutionState.NotFound;
    }
}
