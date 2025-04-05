package cz.cuni.matfyz.collector.model;

import java.util.Map;
import java.util.Set;

public interface MapWritableCollection<T extends MapWritable> {
    Set<Map.Entry<String, T>> getItems();
    void AppendTo(Map<String, Object> rootMap, Map<String, Object> itemsMap);
    boolean hasNext();
}
