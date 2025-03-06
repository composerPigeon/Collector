package cz.cuni.matfyz.collector.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class for saving statistical data about result
 */
public class ResultData implements Mappable<String, Object> {

    /** Field containing execution time in millis */
    private Double _executionTime;
    private final TableData _resultTable;

    public ResultData() {
        _resultTable = new TableData("");
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

    // Column data setters
    public void setColumnTypeByteSize(String columnName, String columnType, int size) {
        _resultTable.setColumnTypeByteSize(columnName, columnType, size);
    }
    public void addColumnType(String columnName, String columnType) {
        _resultTable.addColumnType(columnName, columnType);
    }
    public void setColumnTypeRatio(String columnName, String columnType, double ratio) {
        _resultTable.setColumnTypeRatio(columnName, columnType, ratio);
    }

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
