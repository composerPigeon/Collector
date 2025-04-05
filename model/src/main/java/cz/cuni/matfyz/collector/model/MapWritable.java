package cz.cuni.matfyz.collector.model;

import java.util.Map;

public interface MapWritable {
    void WriteTo(Map<String, Object> map);
}
