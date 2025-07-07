package cz.cuni.matfyz.collector.server.configurationproperties;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import org.springframework.boot.context.properties.bind.ConstructorBinding;

/**
 * Class representing all information about connecting to instances defined in application.properties. It is used to initialize wrappers.
 */
public class WrapperInstance extends AbstractInstance<AbstractWrapper.ConnectionData> {

    @ConstructorBinding
    public WrapperInstance(
            SystemType systemType,
            String instanceName,
            String hostName,
            int port,
            String databaseName,
            String userName,
            String password
    ) {
        super(
                new Identifier(systemType, instanceName),
                hostName,
                port,
                databaseName,
                new AbstractInstance.Credentials(userName, password)
        );
    }

    public Identifier getIdentifier() {
        return _propsID;
    }

    @Override
    public AbstractWrapper.ConnectionData getConnectionData() {
        return new AbstractWrapper.ConnectionData(
                _hostName,
                _port,
                _propsID.systemType().name(),
                _databaseName,
                _credentials.userName(),
                _credentials.password()
        );
    }
}
