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

    public List<Instance> getInstances() {
        return _wrappers;
    }

    public boolean contains(String instanceName) {
        return _wrappers.stream().anyMatch(instance -> instance.getInstanceName().equals(instanceName));
    }

    public Instance getByName(String instanceName) {
        return _wrappers.stream().filter(instance -> instance.getInstanceName().equals(instanceName)).findFirst().orElse(null);
    }
}
