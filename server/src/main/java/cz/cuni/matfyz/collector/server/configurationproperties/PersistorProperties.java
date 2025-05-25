package cz.cuni.matfyz.collector.server.configurationproperties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.bind.ConstructorBinding;

/**
 * Class representing all information about connecting to instances defined in application.properties
 */
@ConfigurationProperties(prefix = "persistor")
@ConfigurationPropertiesScan
public class PersistorProperties {
    /** Field containing hostName to connect to persistor */
    private final String _hostName;
    /** Field containing hostName to connect to persistor */
    private final int _port;
    /** Field containing datasetName to specify which to use in persistor */
    private final String _databaseName;
    private final Credentials _credentials;

    @ConstructorBinding
    public PersistorProperties(String hostName, int port, String databaseName, Credentials credentials) {
        _hostName = hostName;
        _port = port;
        _databaseName = databaseName;
        _credentials = credentials;
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
