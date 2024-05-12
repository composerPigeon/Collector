package cz.cuni.matfyz.collector.server.configurationproperties;

import org.springframework.boot.context.properties.bind.ConstructorBinding;

public class Instance {
    private final DBType _dbType;
    private final String _instanceName;
    private final String _hostName;
    private final int _port;
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
