package cz.cuni.matfyz.collector.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.util.Map;
import java.util.List;

@RestController
public class QueryController {

    @Autowired
    private ExecutionsManager _manager;

    @Autowired
    private WrappersContainer _wrappers;

    private final Logger _logger = LoggerFactory.getLogger(QueryController.class);

    @PostMapping("/query")
    public String createQuery(@RequestBody Map<String, Object> request) {
        try {
            if (request.containsKey("instance") && request.containsKey("query")) {
                String instanceName = (String)request.get("instance");
                String query = (String)request.get("query");
                return _manager.createExecution(instanceName, query);
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
    public String getStatus(@PathVariable("id") String executionId) {
        try {
            ExecutionState state = _manager.getExecutionState(executionId);
            return switch (state) {
                case Running -> "Running";
                case Waiting -> "Waiting";
                case Processed -> "Processed";
                case NotFound -> throw new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Execution " + executionId + " wasn't found."
                );
            };
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

    @GetMapping("/query/{id}/result")
    public String getResult(@PathVariable("id") String executionId) {
        try {
            String result = _manager.getExecutionResult(executionId);
            if (result == null)
                throw new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Execution " + executionId + " wasn't found."
                );
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

    @GetMapping("/instances/list")
    public String getWrappers() {
        try {
            List<Map<String, Object>> list = _wrappers.list();
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(list);
        } catch (JsonProcessingException e) {
            _logger.atError().setCause(e).log(e.getMessage());
            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    ErrorMessages.serializeWrappersErrorMsg()
            );
        } catch (Exception e) {
            _logger.atError().setCause(e).log(e.getMessage());
            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    ErrorMessages.unexpectedErrorMsg()
            );
        }
    }


}
