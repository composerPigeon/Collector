package cz.cuni.matfyz.collector.server.configurationproperties;

public abstract class AbstractInstance<TConnectionData> {
    protected final Identifier _propsID;
    protected final String _hostName;
    protected final int _port;
    protected final String _databaseName;
    protected final String _userName;
    protected final String _password;

    protected AbstractInstance(Identifier propsID, String hostName, int port, String databaseName, String userName, String password) {
        _propsID = propsID;
        _hostName = hostName;
        _port = port;
        _databaseName = databaseName;
        _userName = userName;
        _password = password;
    }

    public String getInstanceName() {
        return _propsID.instanceName();
    }

    public SystemType getSystemType() {
        return _propsID.systemType();
    }

    public abstract TConnectionData getConnectionData();

    public boolean equals(String other) {
        return getInstanceName().equals(other);
    }

    public record Identifier(SystemType systemType, String instanceName) {}
}
