package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.server.executions.Execution;
import cz.cuni.matfyz.collector.server.executions.ExecutionsManager;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@EnableAsync
@Component
public class QueryScheduler {

    @Autowired
    private WrappersContainer _wrappers;

    @Autowired
    private ExecutionsManager _manager;

    @Async
    @Scheduled(fixedRate = 50)
    public void execute() {
        try {
            Execution execution = _manager.getExecutionFromQueue();

            if (execution != null) {
                if (_wrappers.contains(execution.instanceName())) {
                    _manager.setExecutionRunning(execution.uuid());
                    DataModel result = _wrappers.executeQuery(execution.instanceName(), execution.query());
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
