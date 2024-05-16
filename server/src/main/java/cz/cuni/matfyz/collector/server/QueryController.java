package cz.cuni.matfyz.collector.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cuni.matfyz.collector.server.executions.ExecutionState;
import cz.cuni.matfyz.collector.server.executions.ExecutionsManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;
import java.util.List;
import java.util.concurrent.Executor;

@RestController
public class QueryController {

    @Autowired
    private ExecutionsManager _manager;

    @Autowired
    private WrappersContainer _wrappers;

    @PostMapping("/query")
    public String createQuery(@RequestBody Map<String, Object> request) {
        try {
            if (request.containsKey("instance") && request.containsKey("query")) {
                String instanceName = (String)request.get("instance");
                String query = (String)request.get("query");
                return _manager.createExecution(instanceName, query);
            } else {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "Request has to contain fields instance and query."
                );
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @GetMapping("/query/{id}/state")
    public String getStatus(@PathVariable("id") String executionId) {
        ExecutionState state = _manager.getExecutionState(executionId);
        return switch (state) {
            case Running -> "Running";
            case Waiting -> "Waiting";
            case Processed -> "Processed";
            case NotFound -> throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "Execution " + executionId + " wasn't found."
            );
        };
    }

    @GetMapping("/query/{id}/result")
    public String getResult(@PathVariable("id") String executionId) {
        String result = _manager.getExecutionResult(executionId);
        if (result == null)
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "Execution " + executionId + " wasn't found."
            );
        return result;
    }

    @GetMapping("/instances/list")
    public String getWrappers() {
        try {
            List<Map<String, Object>> list = _wrappers.list();
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(list);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "Instances couldn't be serialized."
            );
        }
    }


}
