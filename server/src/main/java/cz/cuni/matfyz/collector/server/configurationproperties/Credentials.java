package cz.cuni.matfyz.collector.server.configurationproperties;

import org.springframework.boot.context.properties.bind.ConstructorBinding;

public class Credentials {
    private final String userName;
    private final String password;

    @ConstructorBinding
    public Credentials(String userName, String password) {
        this.userName = userName;
        this.password = password;
    }

    public String getPassword() {
        return password;
    }

    public String getUserName() {
        return userName;
    }
}
