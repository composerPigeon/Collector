package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.server.executions.Execution;
import cz.cuni.matfyz.collector.server.executions.ExecutionsManager;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;

@EnableAsync
@Component
public class QueryScheduler {

    @Autowired
    private WrappersContainer _wrappersContainer;

    @Autowired
    private ExecutionsManager _manager;

    @Async
    @Scheduled(fixedRate = 5000)
    public void execute() {
        try {
            Execution execution = _manager.getExecutionFromQueue();

            if (execution != null) {
                System.out.println(execution);
                Map<String, AbstractWrapper> wrappers = _wrappersContainer.getWrappers();
                if (wrappers.containsKey(execution.instanceName())) {
                    DataModel result = wrappers.get(execution.instanceName()).executeQuery(execution.query());
                    System.out.println(result.toJson());
                    _manager.saveResult(execution.uuid(), result);
                }

                System.out.println("Execution " + execution.uuid() + " was successfully executed");
            } else {
                System.out.println("No executions to be executed.");
            }

        } catch (WrapperException e) {
            e.printStackTrace();
        }
    }
}
