package cz.cuni.matfyz.collector.server.configurationproperties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.bind.ConstructorBinding;

import java.util.HashMap;
import java.util.List;

/**
 * Class that initialize all instances into list
 */
@ConfigurationProperties
@ConfigurationPropertiesScan
public class WrapperInstanceList {
    private final List<WrapperInstance> _wrappers;

    @ConstructorBinding
    public WrapperInstanceList(List<WrapperInstance> wrappers) {
        _wrappers = wrappers;

        var map = new HashMap<String, WrapperInstance>();
        for (WrapperInstance instance : _wrappers) {
            if (map.containsKey(instance.getInstanceName())) {
                throw new IllegalArgumentException("Duplicate instance name for wrappers: " + instance.getInstanceName());
            }
            map.put(instance.getInstanceName(), instance);
        }
    }

    public List<WrapperInstance> getInstances() {
        return _wrappers;
    }

    public boolean contains(String instanceName) {
        return _wrappers.stream().anyMatch(instance -> instance.equals(instanceName));
    }

    public WrapperInstance getByName(String instanceName) {
        return _wrappers.stream().filter(instance -> instance.equals(instanceName)).findFirst().orElse(null);
    }
}
