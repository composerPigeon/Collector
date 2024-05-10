package cz.cuni.matfyz.collector.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import cz.cuni.matfyz.collector.model.DataModel;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
public class HelloController {

    @PostMapping("/")
    public String executeQuery(@RequestBody Map<String, Object> request) {
        try {
            if (request.containsKey("instance") && request.containsKey("query")) {
                String instanceName = (String)request.get("instance");
                String query = (String)request.get("query");
                DataModel model = QueryExecutor.getInstance().execute(instanceName, query);
                System.out.println(model.toJson());
                return "Query was successfully executed";
            } else {
                return "Invalid body";
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return e.getMessage();
        }

    }


}
