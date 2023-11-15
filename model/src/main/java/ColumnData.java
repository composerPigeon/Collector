package model.src.main.java;

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

    public String toString(int indent) {
        String indentStr = QueryData.createIndentedString(indent);
        StringBuilder str = new StringBuilder();

        str.append(indentStr + "Column name: " + name + "\n");
        str.append(indentStr + "Column size: " + size + "\n");
        str.append(indentStr + "Column distinct ratio: " + ratio + "\n");

        return str.toString();
    }
}
