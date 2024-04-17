package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

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

}
