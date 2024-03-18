package cz.cuni.matfyz.collector.model;

public class ResultData {

    private Double _executionTime;
    private final TableData _resultTable;

    public ResultData() {
        _resultTable = new TableData("result");
        _executionTime = null;
    }

    public void setByteSize(int size) {
        _resultTable.setByteSize(size);
    }

    public void setSizeInPages(int sizeInPages) {
        _resultTable.setSizeInPages(sizeInPages);
    }

    public void setRowCount(int count) {
        _resultTable.setRowCount(count);
    }

    public void setExecutionTime(double time) {
        if (_executionTime == null) _executionTime = time;
    }
}
