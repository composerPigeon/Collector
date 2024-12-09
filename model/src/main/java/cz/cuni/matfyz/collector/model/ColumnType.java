package cz.cuni.matfyz.collector.model;

import java.util.LinkedHashMap;
import java.util.Map;

public class ColumnType implements Mappable<String, Object> {
    private final String _typeName;
    private Integer _byteSize;

    public ColumnType(String typeName) {
        _typeName = typeName;
    }

    public void setByteSize(int size) {
        if (_byteSize == null)
            _byteSize = size;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if (_typeName != null)
            map.put("typeName", _typeName);
        if (_byteSize != null)
            map.put("byteSize", _byteSize);
        return map;
    }
}
