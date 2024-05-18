package cz.cuni.matfyz.collector.server.configurationproperties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.bind.ConstructorBinding;

import java.util.List;

/**
 * Class that initialize all instances into list
 */
@ConfigurationProperties
@ConfigurationPropertiesScan
public class WrappersProperties {
    private final List<Instance> _wrappers;

    @ConstructorBinding
    public WrappersProperties(List<Instance> wrappers) {
        _wrappers = wrappers;
    }

    public List<Instance> getWrappers() {
        return _wrappers;
    }
}
