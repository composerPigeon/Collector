package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.server.executions.ExecutionsManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
public class QueryController {

    @Autowired
    private ExecutionsManager _manager;

    @PostMapping("/")
    public String createQuery(@RequestBody Map<String, Object> request) {
        try {
            if (request.containsKey("instance") && request.containsKey("query")) {
                String instanceName = (String)request.get("instance");
                String query = (String)request.get("query");
                return _manager.createExecution(instanceName, query);
            } else {
                return "Invalid body";
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @GetMapping("/")
    public String getQuery(@RequestBody String executionId) {
         return _manager.getExecutionState(executionId);
    }


}
