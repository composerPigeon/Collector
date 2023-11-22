package cz.cuni.matfyz.collector.model;

public class ColumnData {

    private String name;
    private int size; //size in bytes
    private double ratio; // ratio of distinct values in colum, works as in PostgreSQL

    public ColumnData(String name) {
        this.name = name;
    }

    public void setByteSize(int size) {
        this.size = size;
    }

    public void setDistinctRatio(double ratio) {
        this.ratio = ratio;
    }

}
