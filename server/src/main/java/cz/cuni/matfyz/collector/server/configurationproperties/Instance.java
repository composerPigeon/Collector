package cz.cuni.matfyz.collector.server.configurationproperties;

import org.springframework.boot.context.properties.bind.ConstructorBinding;

/**
 * Class representing all information about connecting to instances defined in application.properties. It is used to initialize wrappers.
 */
public class Instance {
    /** Field holding database type based on which wrapper implementation will be picked up */
    private final DBType _dbType;
    /** Field holding instance name specified by user */
    private final String _instanceName;
    /** Field holding hostName to connect to database */
    private final String _hostName;
    /** Field holding port to connect to database */
    private final int _port;
    /** Field holding datasetName to connect to database */
    private final String _datasetName;
    private final Credentials _credentials;

    @ConstructorBinding
    public Instance(DBType dbType, String instanceName, String hostName, int port, String datasetName, Credentials credentials) {
        _dbType = dbType;
        _instanceName = instanceName;
        _hostName = hostName;
        _port = port;
        _datasetName = datasetName;
        _credentials = credentials;
    }

    public DBType getDbType() {
        return _dbType;
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

    public String getDatasetName() {
        return _datasetName;
    }

    public Credentials getCredentials() {
        return _credentials;
    }
}
