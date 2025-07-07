package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.persistor.AbstractPersistor;
import cz.cuni.matfyz.collector.persistor.MongoPersistor;
import cz.cuni.matfyz.collector.persistor.PersistorException;
import cz.cuni.matfyz.collector.server.configurationproperties.PersistorInstance;
import cz.cuni.matfyz.collector.server.configurationproperties.WrapperInstance;
import cz.cuni.matfyz.collector.server.configurationproperties.SystemType;
import cz.cuni.matfyz.collector.server.exceptions.ErrorMessages;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.Wrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import cz.cuni.matfyz.collector.wrappers.mongodb.MongoWrapper;
import cz.cuni.matfyz.collector.wrappers.neo4j.Neo4jWrapper;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresWrapper;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@Component
public class Initializers {
    private final Map<SystemType, Function<AbstractWrapper.ConnectionData, Wrapper>> _wrapperInitializers;
    private final Map<SystemType, Function<AbstractPersistor.ConnectionData, AbstractPersistor>> _persistorInitializers;
    private final ErrorMessages _errors;

    @Autowired
    public Initializers(ErrorMessages errorMessages) {
        _wrapperInitializers = new HashMap<>();
        _persistorInitializers = new HashMap<>();
        _errors = errorMessages;
    }

    public void registerWrapper(SystemType type, Function<AbstractWrapper.ConnectionData, Wrapper> initializer) {
        _wrapperInitializers.put(type, initializer);
    }

    public void registerPersistor(SystemType type, Function<AbstractPersistor.ConnectionData, AbstractPersistor> initializer) {
        _persistorInitializers.put(type, initializer);
    }

    public Wrapper initializeWrapper(SystemType type, WrapperInstance instance) throws WrapperException {
        if (!_wrapperInitializers.containsKey(type)) {
            throw new WrapperException(_errors.missingWrapperInitializer(type));
        }
        return _wrapperInitializers.get(type).apply(instance.getConnectionData());
    }

    public AbstractPersistor initializePersistor(SystemType type, PersistorInstance properties) throws PersistorException {
        if (!_persistorInitializers.containsKey(type)) {
            throw new PersistorException(_errors.missingPersistorInitializer(type));
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
