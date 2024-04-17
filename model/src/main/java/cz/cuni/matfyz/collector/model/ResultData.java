package cz.cuni.matfyz.collector.model;

public class ResultData {

    private Double _executionTime;
    private final TableData _resultTable;

    public ResultData() {
        _resultTable = new TableData("");
        _executionTime = null;
    }

    public void setByteSize(long size) {
        _resultTable.setByteSize(size);
    }

    public void setSizeInPages(long sizeInPages) {
        _resultTable.setSizeInPages(sizeInPages);
    }

    public void setRowCount(long count) {
        _resultTable.setRowCount(count);
    }

    public void setExecutionTime(double time) {
        if (_executionTime == null) _executionTime = time;
    }

    // Column data
    public void setColumnByteSize(String columnName, int size) {
        _resultTable.setColumnByteSize(columnName, size);
    }
    public void setColumnType(String columnName, String columnType) {
        _resultTable.setColumnType(columnName, columnType);
    }
}
