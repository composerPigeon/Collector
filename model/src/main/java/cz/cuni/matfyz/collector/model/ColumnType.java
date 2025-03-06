package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.LinkedHashMap;
import java.util.Map;

public class ColumnType implements Mappable<String, Object> {
    private final String _typeName;
    private Integer _byteSize;
    private Double _ratio;

    public ColumnType(String typeName) {
        _typeName = typeName;
    }

    @JsonIgnore
    public int getByteSize() { return _byteSize; }

    public void setByteSize(int size) {
        if (_byteSize == null)
            _byteSize = size;
    }

    public void setRatio(double ratio) {
        if (_ratio == null)
            _ratio = ratio;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if (_typeName != null)
            map.put("typeName", _typeName);
        if (_byteSize != null)
            map.put("byteSize", _byteSize);
        if (_ratio != null)
            map.put("ratio", _ratio);
        return map;
    }
}
