package cz.cuni.matfyz.collector.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class for saving statistical data about result
 */
public class ResultData {

    /** Field containing execution time in millis */
    private Double _executionTime;
    private final TableData _resultTable;

    public ResultData() {
        _resultTable = new TableData();
        _executionTime = null;
    }

    // Table data setters
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

    public ColumnData getColumn(String columnName, boolean createIfNotExist) throws IllegalArgumentException { return _resultTable.getColumn(columnName, createIfNotExist); }

    /**
     * Method for parsing ResultData to map for saving via org.bson.Document
     * @return converted map
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if (_executionTime != null)
            map.put("executionTime", _executionTime);
        map.put("resultTable", _resultTable.toMap());
        return map;
    }
}
