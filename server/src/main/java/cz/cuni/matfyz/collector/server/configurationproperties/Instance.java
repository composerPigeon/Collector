package cz.cuni.matfyz.collector.server.configurationproperties;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import org.springframework.boot.context.properties.bind.ConstructorBinding;

/**
 * Class representing all information about connecting to instances defined in application.properties. It is used to initialize wrappers.
 */
public class Instance {
    private final ID _instanceID;
    /** Field holding hostName to connect to database */
    private final String _hostName;
    /** Field holding port to connect to database */
    private final int _port;
    /** Field holding datasetName to connect to database */
    private final String _databaseName;
    private final Credentials _credentials;

    @ConstructorBinding
    public Instance(SystemType systemType, String instanceName, String hostName, int port, String databaseName, Credentials credentials) {
        _instanceID = new ID(systemType, instanceName);
        _hostName = hostName;
        _port = port;
        _databaseName = databaseName;
        _credentials = credentials;
    }

    public String getInstanceName() {
        return _instanceID.instanceName();
    }

    public SystemType getSystemType() {
        return _instanceID.systemType();
    }

    public ID getID() {
        return _instanceID;
    }

    public AbstractWrapper.ConnectionData getConnectionData() {
        return new AbstractWrapper.ConnectionData(
                _hostName,
                _port,
                _instanceID.systemType.name(),
                _databaseName,
                _credentials.userName(),
                _credentials.password()
        );
    }

    public record ID(SystemType systemType, String instanceName) {}
}
