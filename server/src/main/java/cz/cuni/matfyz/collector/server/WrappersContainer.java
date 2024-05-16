package cz.cuni.matfyz.collector.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.server.configurationproperties.Instance;
import cz.cuni.matfyz.collector.server.configurationproperties.WrappersProperties;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;
import cz.cuni.matfyz.collector.wrappers.mongodb.MongoWrapper;
import cz.cuni.matfyz.collector.wrappers.neo4j.Neo4jWrapper;
import cz.cuni.matfyz.collector.wrappers.postgresql.PostgresWrapper;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

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

    public List<Map<String, Object>> list() {
        List<Map<String, Object>> list = new ArrayList<>();

        for (var entry : _wrappers.entrySet()) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("instanceName", entry.getKey());
            if (entry.getValue() instanceof MongoWrapper)
                map.put("type", "MongoDB");
            else if (entry.getValue() instanceof Neo4jWrapper)
                map.put("type", "Neo4j");
            else if (entry.getValue() instanceof PostgresWrapper)
                map.put("type", "PostgreSQL");
            list.add(map);
        }

        return list;
    }

    public boolean contains(String instanceName) {
        return _wrappers.containsKey(instanceName);
    }
    public DataModel executeQuery(String instanceName, String query) throws WrapperException {
        return _wrappers.get(instanceName).executeQuery(query);
    }
}
