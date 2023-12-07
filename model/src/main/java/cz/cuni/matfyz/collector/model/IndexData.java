package cz.cuni.matfyz.collector.model;

public class IndexData {
    private String _name;

    public int _size; //size of table in bytes
    public int _sizeInPages; //size of tables in pages on disk
    public int _rowCount; //number of rows

    public IndexData(String name) {
        _name = name;
        _size = -1;
        _sizeInPages = -1;
        _rowCount = -1;
    }

    //Tables setting methods
    public void setByteSize(int size) {
        if (_size == -1) {_size = size;}
    }

    public void setSizeInPages(int sizeInPages) {
        if (_sizeInPages == -1) {_sizeInPages = sizeInPages;}
    }

    public void setRowCount(int count) {
        if (_rowCount == -1) {_rowCount = count;}
    }
}
