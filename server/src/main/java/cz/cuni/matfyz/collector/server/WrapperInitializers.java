package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.server.configurationproperties.Instance;
import cz.cuni.matfyz.collector.server.configurationproperties.SystemType;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.Wrapper;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@Component
public class WrapperInitializers {
    private final Map<SystemType, Function<AbstractWrapper.ConnectionData, Wrapper>> _initializers;

    public WrapperInitializers() {
        _initializers = new HashMap<>();
    }

    public void register(SystemType type, Function<AbstractWrapper.ConnectionData, Wrapper> initializer) {
        _initializers.put(type, initializer);
    }

    public Wrapper initialize(SystemType type, Instance instance) {
        if (!_initializers.containsKey(type)) {
            throw new IllegalArgumentException("No initializer for type " + type);
        }
        return _initializers.get(type).apply(instance.getConnectionData());
    }
}
