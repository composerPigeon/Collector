package cz.cuni.matfyz.collector.server;

import cz.cuni.matfyz.collector.persistor.AbstractPersistor;
import cz.cuni.matfyz.collector.persistor.MongoPersistor;
import cz.cuni.matfyz.collector.server.configurationproperties.PersistorProperties;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PersistorContainer {

    @Autowired
    private PersistorProperties _properties;

    private AbstractPersistor _persistor;

    @PostConstruct
    public void init() {
        _persistor = new MongoPersistor(
                _properties.getHostName(),
                _properties.getPort(),
                _properties.getDatasetName(),
                _properties.getCredentials().getUserName(),
                _properties.getCredentials().getPassword()
        );
    }

    public AbstractPersistor getPersistor() {
        return _persistor;
    }
}
