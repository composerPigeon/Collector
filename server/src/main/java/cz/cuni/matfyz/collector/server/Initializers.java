package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.persistor.AbstractPersistor;
import cz.cuni.matfyz.collector.persistor.MongoPersistor;
import cz.cuni.matfyz.collector.server.configurationproperties.Instance;
import cz.cuni.matfyz.collector.server.configurationproperties.PersistorProperties;
import cz.cuni.matfyz.collector.server.configurationproperties.SystemType;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.Wrapper;
import cz.cuni.matfyz.collector.wrappers.mongodb.MongoWrapper;
import cz.cuni.matfyz.collector.wrappers.neo4j.Neo4jWrapper;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresWrapper;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@Component
public class Initializers {
    private final Map<SystemType, Function<AbstractWrapper.ConnectionData, Wrapper>> _wrapperInitializers;
    private final Map<SystemType, Function<AbstractPersistor.ConnectionData, AbstractPersistor>> _persistorInitializers;

    public Initializers() {
        _wrapperInitializers = new HashMap<>();
        _persistorInitializers = new HashMap<>();
    }

    public void registerWrapper(SystemType type, Function<AbstractWrapper.ConnectionData, Wrapper> initializer) {
        _wrapperInitializers.put(type, initializer);
    }

    public void registerPersistor(SystemType type, Function<AbstractPersistor.ConnectionData, AbstractPersistor> initializer) {
        _persistorInitializers.put(type, initializer);
    }

    public Wrapper initializeWrapper(SystemType type, Instance instance) {
        if (!_wrapperInitializers.containsKey(type)) {
            throw new IllegalArgumentException("No initializer for wrapper of type " + type);
        }
        return _wrapperInitializers.get(type).apply(instance.getConnectionData());
    }

    public AbstractPersistor initializePersistor(SystemType type, PersistorProperties properties) {
        if (!_persistorInitializers.containsKey(type)) {
            throw new IllegalArgumentException("No initializer for persistor of type " + type);
        }
        return _persistorInitializers.get(type).apply(properties.getConnectionData());
    }

    @PostConstruct
    private void _initialize() {
        registerWrapper(SystemType.MongoDB, MongoWrapper::new);
        registerWrapper(SystemType.Neo4j, Neo4jWrapper::new);
        registerWrapper(SystemType.PostgreSQL, PostgresWrapper::new);

        registerPersistor(SystemType.MongoDB, MongoPersistor::new);
    }
}
