package cz.cuni.matfyz.collector.server.configurationproperties;

import cz.cuni.matfyz.collector.persistor.AbstractPersistor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.bind.ConstructorBinding;

/**
 * Class representing all information about connecting to instances defined in application.properties
 */
@ConfigurationProperties(prefix = "persistor")
@ConfigurationPropertiesScan
public class PersistorInstance extends AbstractInstance<AbstractPersistor.ConnectionData> {

    @ConstructorBinding
    public PersistorInstance(
            SystemType systemType,
            String hostName,
            int port,
            String databaseName,
            String userName,
            String password
    ) {
        super(
                new Identifier(systemType, "persistor"),
                hostName,
                port,
                databaseName,
                userName,
                password
        );
    }

    public AbstractPersistor.ConnectionData getConnectionData() {
        return new AbstractPersistor.ConnectionData(
                _hostName,
                _port,
                _databaseName,
                _userName,
                _password
        );
    }
}
