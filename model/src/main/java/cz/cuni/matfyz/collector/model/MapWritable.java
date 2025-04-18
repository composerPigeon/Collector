package cz.cuni.matfyz.collector.model;

import java.util.Map;

public interface MapWritable {
    void writeTo(Map<String, Object> map);
}
