package cz.cuni.matfyz.collector.server.executions;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.server.WrappersContainer;
import cz.cuni.matfyz.collector.server.exceptions.ErrorMessages;
import cz.cuni.matfyz.collector.server.exceptions.ExecutionManagerException;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import org.slf4j.Logger;

/**
 * Class representing Scheduler that will execute all waiting executions from queue
 */
@Component
@EnableScheduling
public class ExecutionsScheduler {

    private final WrappersContainer _wrappers;

    private final ExecutionsManager _manager;

    private final Logger _logger;

    private final ErrorMessages _errors;

    @Autowired
    public ExecutionsScheduler(WrappersContainer wrappers, ExecutionsManager manager, ErrorMessages errorMessages) {
        _wrappers = wrappers;
        _manager = manager;
        _logger = LoggerFactory.getLogger(ExecutionsScheduler.class);
        _errors = errorMessages;
    }

    /**
     * Scheduled method for executing all waiting executions from queue
     */
    @Scheduled(fixedRate = 5000)
    public void runExecutions() {
        try {
            for (Execution execution : _manager.getWaitingExecutionsFromQueue()) {
                try {
                    if (_wrappers.contains(execution.instanceName())) {
                        _manager.setExecutionRunning(execution.uuid());
                        DataModel result = _wrappers.executeQuery(execution.instanceName(), execution.query());
                        _manager.saveExecutionResult(execution.uuid(), result);
                    } else {
                        _logger.atError().log(_errors.nonExistentWrapper(execution.uuid(), execution.instanceName()));
                        _manager.saveExecutionError(execution.uuid(), _errors.nonExistentWrapper(execution.uuid(), execution.instanceName()));
                    }
                    _logger.atInfo().log("Execution " + execution.uuid() + " was successfully executed");
                } catch (WrapperException | ExecutionManagerException e) {
                    var message = _errors.executionOfWrapperFailed(execution, e);

                    _logger.atError().setCause(e).log(message);
                    _manager.saveExecutionError(execution.uuid(), message);
                } catch (Exception e) {
                    _logger.atError().setCause(e).log(e.getMessage());
                    _manager.saveExecutionError(execution.uuid(), _errors.unexpectedErrorMsg());
                }
            }
        } catch (Exception e) {
            _logger.atError().setCause(e).log(e.getMessage());
        }
    }
}
