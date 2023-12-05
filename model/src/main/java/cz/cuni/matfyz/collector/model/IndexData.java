package cz.cuni.matfyz.collector.model;

public class IndexData {
    private String _name;

    public int _size; //size of table in bytes
    public int _sizeInPages; //size of tables in pages on disk
    public int _rowCount; //number of rows

    public IndexData(String name) {
        _name = name;
    }

    //Tables setting methods
    public void setByteSize(int size) {
        _size = size;
    }

    public void setSizeInPages(int sizeInPages) {
        _sizeInPages = sizeInPages;
    }

    public void setRowCount(int count) {
        _rowCount = count;
    }
}
