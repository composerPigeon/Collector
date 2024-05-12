package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.server.configurationproperties.Instance;
import cz.cuni.matfyz.collector.server.configurationproperties.WrappersProperties;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.mongodb.MongoWrapper;
import cz.cuni.matfyz.collector.wrappers.neo4j.Neo4jWrapper;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresWrapper;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class WrappersContainer {

    @Autowired
    private WrappersProperties _properties;

    private Map<String, AbstractWrapper> _wrappers;

    @PostConstruct
    public void init() {
        _wrappers = new HashMap<>();
        for (Instance instance : _properties.getWrappers()) {
            switch (instance.getDbType()) {
                case PostgreSQL -> _wrappers.put(instance.getInstanceName(), new PostgresWrapper(
                        instance.getHostName(),
                        instance.getPort(),
                        instance.getDatasetName(),
                        instance.getCredentials().getUserName(),
                        instance.getCredentials().getPassword()
                ));
                case Neo4j -> _wrappers.put(instance.getInstanceName(), new Neo4jWrapper(
                        instance.getHostName(),
                        instance.getPort(),
                        instance.getDatasetName(),
                        instance.getCredentials().getUserName(),
                        instance.getCredentials().getPassword()
                ));
                case MongoDB -> _wrappers.put(instance.getInstanceName(), new MongoWrapper(
                        instance.getHostName(),
                        instance.getPort(),
                        instance.getDatasetName(),
                        instance.getCredentials().getUserName(),
                        instance.getCredentials().getPassword()
                ));
            }
        }
    }

    public Map<String, AbstractWrapper> getWrappers() {
        return _wrappers;
    }
}
