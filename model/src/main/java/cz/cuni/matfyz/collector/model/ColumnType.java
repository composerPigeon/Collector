package cz.cuni.matfyz.collector.model;

import java.util.LinkedHashMap;
import java.util.Map;

public class ColumnType {
    private Integer _byteSize;
    private Double _ratio;

    public ColumnType() {
        _byteSize = null;
        _ratio = null;
    }

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
        var map = new LinkedHashMap<String, Object>();
        if (_byteSize != null)
            map.put("byteSize", _byteSize);
        if (_ratio != null)
            map.put("ratio", _ratio);
        return map;
    }
}
