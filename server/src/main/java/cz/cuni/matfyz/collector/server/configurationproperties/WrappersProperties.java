package cz.cuni.matfyz.collector.server.configurationproperties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.bind.ConstructorBinding;

import java.util.Set;

/**
 * Class that initialize all instances into list
 */
@ConfigurationProperties
@ConfigurationPropertiesScan
public class WrappersProperties {
    private final Set<Instance> _wrappers;

    @ConstructorBinding
    public WrappersProperties(Set<Instance> wrappers) {
        _wrappers = wrappers;
    }

    public Set<Instance> getInstances() {
        return _wrappers;
    }

    public boolean contains(String instanceName) {
        return _wrappers.stream().anyMatch(instance -> instance.equals(instanceName));
    }

    public Instance getByName(String instanceName) {
        return _wrappers.stream().filter(instance -> instance.equals(instanceName)).findFirst().orElse(null);
    }
}
