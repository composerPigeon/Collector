package cz.cuni.matfyz.collector.model;

import java.util.Map;

public interface Mappable<K, V> {
    public Map<K, V> toMap();
}
