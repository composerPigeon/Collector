package cz.cuni.matfyz.collector.model;

public class IndexData {
    private String name;

    public int size; //size of table in bytes
    public int sizeInPages; //size of tables in pages on disk
    public int rowCount; //number of rows

    public IndexData(String name) {
        this.name = name;
    }

    //Tables setting methods
    public void setByteSize(int size) {
        this.size = size;
    }

    public void setSizeInPages(int sizeInPages) {
        this.sizeInPages = sizeInPages;
    }

    public void setRowCount(int count) {
        rowCount = count;
    }
}
