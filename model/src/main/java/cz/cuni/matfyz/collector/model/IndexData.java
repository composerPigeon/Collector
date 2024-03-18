package cz.cuni.matfyz.collector.model;

public class IndexData {
    private final String _name;
    public Integer _size; //size of table in bytes
    public Integer _sizeInPages; //size of tables in pages on disk
    public Integer _rowCount; //number of rows

    public IndexData(String name) {
        _name = name;
        _size = null;
        _sizeInPages = null;
        _rowCount = null;
    }

    //Tables setting methods
    public void setByteSize(int size) {
        if (_size == null) {_size = size;}
    }

    public void setSizeInPages(int sizeInPages) {
        if (_sizeInPages == null) {_sizeInPages = sizeInPages;}
    }

    public void setRowCount(int count) {
        if (_rowCount == null) {_rowCount = count;}
    }
}
