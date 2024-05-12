package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.LinkedHashMap;
import java.util.Map;

public class ColumnData {

    private Integer _size; //size in bytes
    private Double _ratio;
    private String _type;
    private Boolean _mandatory;// ratio of distinct values in colum, works as in PostgreSQL

    public ColumnData() {
        _size = null;
        _ratio = null;
        _type = null;
        _mandatory = null;
    }

    public void setByteSize(int size) {
        if (_size == null)
            _size = size;
    }
    @JsonIgnore
    public int getByteSize() { return _size; }

    public void setType(String type) {
        if (_type == null)
            _type = type;
    }

    public void setMandatory(boolean value) {
        if (_mandatory == null)
            _mandatory = value;
    }

    public void setDistinctRatio(double ratio) {
        if (_ratio == null) {_ratio = ratio;}
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if (_size != null)
            map.put("size", _size);
        if (_ratio != null)
            map.put("ratio", _ratio);
        if (_type != null)
            map.put("type", _type);
        if (_mandatory != null)
            map.put("mandatory", _mandatory);
        return map;
    }

}
