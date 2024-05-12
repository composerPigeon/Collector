package cz.cuni.matfyz.collector.server.configurationproperties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.bind.ConstructorBinding;

@ConfigurationProperties(prefix = "persistor")
@ConfigurationPropertiesScan
public class PersistorProperties {
    private String _hostName;
    private int _port;
    private String _datasetName;
    private Credentials _credentials;

    @ConstructorBinding
    public PersistorProperties(String hostName, int port, String datasetName, Credentials credentials) {
        _hostName = hostName;
        _port = port;
        _datasetName = datasetName;
        _credentials = credentials;
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
