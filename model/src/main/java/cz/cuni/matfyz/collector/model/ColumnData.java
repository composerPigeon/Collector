package cz.cuni.matfyz.collector.model;

public class ColumnData {

    private final String _name;
    private int _size; //size in bytes
    private double _ratio; // ratio of distinct values in colum, works as in PostgreSQL

    public ColumnData(String name) {
        _name = name;
        _size = -1;
        _ratio = Double.NaN;
    }

    public void setByteSize(int size) {
        if (_size == -1) {_size = size;}
    }

    public int getByteSize() { return _size; }

    public void setDistinctRatio(double ratio) {
        if (Double.isNaN(_ratio)) {_ratio = ratio;}
    }

}
