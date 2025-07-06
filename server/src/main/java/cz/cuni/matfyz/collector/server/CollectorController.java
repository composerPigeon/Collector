package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.persistor.ExecutionResult;
import cz.cuni.matfyz.collector.server.configurationproperties.Instance;
import cz.cuni.matfyz.collector.server.exceptions.ErrorMessages;
import cz.cuni.matfyz.collector.server.exceptions.ExecutionManagerException;
import cz.cuni.matfyz.collector.server.executions.ExecutionState;
import cz.cuni.matfyz.collector.server.executions.ExecutionsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class representing all endpoints of server
 */
@RestController
public class CollectorController {

    private final ExecutionsManager _manager;

    private final WrappersContainer _wrappers;

    private final Logger _logger;

    @Autowired
    public CollectorController(ExecutionsManager manager, WrappersContainer wrappers) {
        _manager = manager;
        _wrappers = wrappers;
        _logger = LoggerFactory.getLogger(CollectorController.class);
    }

    /**
     * Endpoint for creating new execution
     * @param request json object that will be parsed into Map(String, Object)
     * @return newly created execution's id
     */
    @PostMapping("/query")
    public Map<String, String> createExecution(@RequestBody Map<String, Object> request) {
        try {
            if (request.containsKey("instance") && request.containsKey("query")) {
                String instanceName = (String)request.get("instance");
                String query = (String)request.get("query");
                var id = _manager.createExecution(instanceName, query);
                return Map.of("executionId", id);
            } else {
                _logger.atError().log(ErrorMessages.badCreateRequestErrorMsg());
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        ErrorMessages.badCreateRequestErrorMsg()
                );
            }
        } catch (ExecutionManagerException e) {
            _logger.atError().setCause(e).log(e.getMessage());
            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    e.getMessage()
            );
        } catch (ResponseStatusException e) {
            throw e;
        } catch (Exception e) {
            _logger.atError().setCause(e).log(e.getMessage());
            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    ErrorMessages.unexpectedErrorMsg()
            );
        }
    }

    @GetMapping("/query/{id}/state")
    public Map<String, String> getState(@PathVariable("id") String executionId) {
        try {
            ExecutionState state = _manager.getExecutionState(executionId);
            Map<String, String> result = new HashMap<>();

            if (state == ExecutionState.NotFound) {
                throw new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        ErrorMessages.nonExistentExecution(executionId)
                );
            }

            result.put("executionId", executionId);
            result.put("state", state.toString());
            return result;

        } catch (ExecutionManagerException e) {
            _logger.atError().setCause(e).log(e.getMessage());
            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    e.getMessage()
            );
        } catch (Exception e) {
            _logger.atError().setCause(e).log(e.getMessage());
            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    ErrorMessages.unexpectedErrorMsg()
            );
        }
    }

    /**
     * Endpoint for getting execution's result
     * @param executionId execution identifier
     * @return json of DataModel or error message
     */
    @GetMapping("/query/{id}/result")
    public Map<String, Object> getResult(@PathVariable("id") String executionId) {
        try {
            Map<String, Object> json = new HashMap<>();
            ExecutionResult result = _manager.getExecutionResult(executionId);
            if (result == null) {
                throw new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        ErrorMessages.nonExistentExecution(executionId)
                );
            } else if (result.isSuccessful()) {
                json.put("model", result.getValue());
            } else {
                json.put("error", result.getErrorMessage());
            }
            json.put("executionId", executionId);
            return json;
        } catch (ExecutionManagerException e) {
            _logger.atError().setCause(e).log(e.getMessage());
            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    e.getMessage()
            );
        } catch (Exception e) {
            _logger.atError().setCause(e).log(e.getMessage());
            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    ErrorMessages.unexpectedErrorMsg()
            );
        }
    }

    /**
     * Endpoint listing all wrappers that can be used for execution
     * @return json list of objects, which contain field 'type' and 'instanceName'
     */
    @GetMapping("/instances/list")
    public Map<String, List<Instance.ID>> getWrappers() {
        try {
            Map<String, List<Instance.ID>> result = new HashMap<>();
            result.put("instances", _wrappers.listInstances());
            return result;
        } catch (Exception e) {
            _logger.atError().setCause(e).log(e.getMessage());
            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    ErrorMessages.unexpectedErrorMsg()
            );
        }
    }


}
