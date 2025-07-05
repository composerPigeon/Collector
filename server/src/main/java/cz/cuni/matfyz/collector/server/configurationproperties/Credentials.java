package cz.cuni.matfyz.collector.server.configurationproperties;

import org.springframework.boot.context.properties.bind.ConstructorBinding;

/**
 * class representing userName and password for instances parsed from application.properties
 */
public record Credentials(String userName, String password) {
    @ConstructorBinding
    public Credentials {}
}
