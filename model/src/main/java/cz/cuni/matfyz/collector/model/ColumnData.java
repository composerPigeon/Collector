package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ColumnData {

    private final String _name;
    private Integer _size; //size in bytes
    private Double _ratio; // ratio of distinct values in colum, works as in PostgreSQL

    public ColumnData(String name) {
        _name = name;
        _size = null;
        _ratio = Double.NaN;
    }

    public void setByteSize(int size) {
        if (_size == null) {_size = size;}
    }

    @JsonIgnore
    public int getByteSize() { return _size; }

    public void setDistinctRatio(double ratio) {
        if (Double.isNaN(_ratio)) {_ratio = ratio;}
    }

}
