package cz.cuni.matfyz.collector.model;

public class IndexData {
    public Long _size; //size of table in bytes
    public Long _sizeInPages; //size of tables in pages on disk
    public Long _rowCount; //number of rows

    public IndexData() {
        _size = null;
        _sizeInPages = null;
        _rowCount = null;
    }

    //Tables setting methods
    public void setByteSize(long size) {
        if (_size == null) {_size = size;}
    }

    public void setSizeInPages(long sizeInPages) {
        if (_sizeInPages == null) {_sizeInPages = sizeInPages;}
    }

    public void setRowCount(long count) {
        if (_rowCount == null) {_rowCount = count;}
    }
}
