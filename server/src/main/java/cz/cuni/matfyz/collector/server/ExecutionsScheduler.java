package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.server.exceptions.ErrorMessages;
import cz.cuni.matfyz.collector.server.exceptions.ExecutionManagerException;
import cz.cuni.matfyz.collector.server.executions.Execution;
import cz.cuni.matfyz.collector.server.executions.ExecutionsManager;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import org.slf4j.Logger;

/**
 * Class representing Scheduler that will execute all waiting executions from queue
 */
@Component
public class ExecutionsScheduler {
    @Autowired
    private WrappersContainer _wrappers;
    @Autowired
    private ExecutionsManager _manager;
    private final Logger _logger = LoggerFactory.getLogger(ExecutionsScheduler.class);

    /**
     * Scheduled method for executing all waiting executions from queue
     */
    @Scheduled(fixedRate = 5000)
    public void execute() {
        try {
            for (Execution execution : _manager.getExecutionsFromQueue()) {
                try {
                    if (_wrappers.contains(execution.instanceName())) {
                        _manager.setExecutionRunning(execution.uuid());
                        DataModel result = _wrappers.executeQuery(execution.instanceName(), execution.query());
                        _manager.saveResult(execution.uuid(), result);
                    } else {
                        _logger.atError().log(ErrorMessages.nonExistentWrapper(execution.uuid(), execution.instanceName()));
                        _manager.saveError(execution.uuid(), ErrorMessages.nonExistentWrapper(execution.uuid(), execution.instanceName()));
                    }
                    _logger.atInfo().log("Execution " + execution.uuid() + " was successfully executed");
                } catch (WrapperException | ExecutionManagerException e) {
                    _logger.atError().setCause(e).log(e.getMessage());
                    _manager.saveError(execution.uuid(), e.getMessage());
                } catch (Exception e) {
                    _logger.atError().setCause(e).log(e.getMessage());
                    _manager.saveError(execution.uuid(), ErrorMessages.unexpectedErrorMsg());
                }
            }
        } catch (ExecutionManagerException e) {
            _logger.atError().setCause(e).log(e.getMessage());
        } catch (Exception e) {
            _logger.atError().setCause(e).log("Error during executing waiting executions from queue.");
        }
    }
}
