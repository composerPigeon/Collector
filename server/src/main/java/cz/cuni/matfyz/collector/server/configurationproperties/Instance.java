package cz.cuni.matfyz.collector.server.configurationproperties;

import org.springframework.boot.context.properties.bind.ConstructorBinding;

/**
 * Class representing all information about connecting to instances defined in application.properties. It is used to initialize wrappers.
 */
public class Instance {
    /** Field holding database type based on which wrapper implementation will be picked up */
    private final SystemType _systemType;
    /** Field holding instance name specified by user */
    private final String _instanceName;
    /** Field holding hostName to connect to database */
    private final String _hostName;
    /** Field holding port to connect to database */
    private final int _port;
    /** Field holding datasetName to connect to database */
    private final String _databaseName;
    private final Credentials _credentials;

    @ConstructorBinding
    public Instance(SystemType systemType, String instanceName, String hostName, int port, String databaseName, Credentials credentials) {
        _systemType = systemType;
        _instanceName = instanceName;
        _hostName = hostName;
        _port = port;
        _databaseName = databaseName;
        _credentials = credentials;
    }

    public SystemType getSystemType() {
        return _systemType;
    }
    public String getInstanceName() {
        return _instanceName;
    }
    public String getHostName() {
        return _hostName;
    }
    public int getPort() {
        return _port;
    }

    public String getDatabaseName() {
        return _databaseName;
    }

    public Credentials getCredentials() {
        return _credentials;
    }
}
