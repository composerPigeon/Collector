package cz.cuni.matfyz.collector.model;

public class ColumnData {

    private String _name;
    private int _size; //size in bytes
    private double _ratio; // ratio of distinct values in colum, works as in PostgreSQL

    public ColumnData(String name) {
        _name = name;
    }

    public void setByteSize(int size) {
        _size = size;
    }

    public void setDistinctRatio(double ratio) {
        _ratio = ratio;
    }

}
