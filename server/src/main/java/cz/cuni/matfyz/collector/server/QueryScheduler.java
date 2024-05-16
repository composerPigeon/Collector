package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.server.exceptions.ExecutionManagerException;
import cz.cuni.matfyz.collector.server.executions.Execution;
import cz.cuni.matfyz.collector.server.executions.ExecutionsManager;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import org.slf4j.Logger;

@Component
public class QueryScheduler {
    @Autowired
    private WrappersContainer _wrappers;
    @Autowired
    private ExecutionsManager _manager;
    private final Logger _logger = LoggerFactory.getLogger(QueryScheduler.class);

    @Scheduled(fixedRate = 5000)
    public void execute() {
        try {
            for (Execution execution : _manager.getExecutionsFromQueue()) {
                try {
                    if (_wrappers.contains(execution.instanceName())) {
                        _manager.setExecutionRunning(execution.uuid());
                        DataModel result = _wrappers.executeQuery(execution.instanceName(), execution.query());
                        _manager.saveResult(execution.uuid(), result);
                    }
                    System.out.println("Execution " + execution.uuid() + " was successfully executed");
                } catch (WrapperException e) {
                    _logger.atError().setCause(e).log("Execution " + execution.uuid() + " couldn't be evaluated.");
                    _manager.saveError(execution.uuid(), e.getMessage());
                } catch (ExecutionManagerException e) {
                    _logger.atError().setCause(e).log(e.getMessage());
                    _manager.saveError(execution.uuid(), e.getMessage());
                }
            }
        } catch (ExecutionManagerException e) {
            _logger.atError().setCause(e).log(e.getMessage());
        } catch (Exception e) {
            _logger.atError().setCause(e).log("Error during executing waiting executions from queue.");
        }
    }
}
