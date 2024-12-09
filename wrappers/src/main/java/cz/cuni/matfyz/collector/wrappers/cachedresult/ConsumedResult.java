package cz.cuni.matfyz.collector.wrappers.cachedresult;

import java.util.*;

/**
 * Class which represents ConsumedResult and provides it unified API to get measured statistical data
 */
public class ConsumedResult {

    /** Field conatining all columnTypes for all columns of result */
    private final Map<String, List<String>> _columnTypes;

    /** Field holding byte size of result measured in bytes */
    private final long _byteSize;

    /** Field containing record count from result */
    private final long _count;

    private ConsumedResult(Map<String, List<String>> columnTypes, long byteSize, long count) {
        _byteSize = byteSize;
        _count = count;
        _columnTypes = columnTypes;
    }

    /**
     * Method for getting all column names present in result
     * @return set of the column names
     */
    public Set<String> getColumnNames() {
        return _columnTypes.keySet();
    }

    /**
     * Method for getting type for specific column
     * @param colName to select column by columnName
     * @return type of this column as string
     */
    public Iterable<String> getColumnTypes(String colName) {
        return _columnTypes.get(colName);
    }

    /**
     * Getter for field _byteSize
     * @return byte size of result
     */
    public long getByteSize() {
        return _byteSize;
    }

    /**
     * Getter for field _rowCount
     * @return row count of result
     */
    public long getRowCount() {
        return _count;
    }

    /**
     * Class representing Builder for ConsumedResult
     */
    public static class Builder {

        private Map<String, List<String>> _columnTypes;
        private long _byteSize;
        private long _count;

        public Builder() {
            _columnTypes = new HashMap<>();
            _count = 0;
            _byteSize = 0;
        }

        /**
         * Method for adding type for specific column
         * @param colName to specify column by columnName
         * @param type inputted type
         */
        public void addColumnType(String colName, String type) {
            if (_columnTypes.containsKey(colName)) {
                _columnTypes.get(colName).add(type);
            } else {
                var list = new ArrayList<String>();
                list.add(type);
                _columnTypes.put(colName, list);
            }
        }

        /**
         * Method for getting info whether data about column specified by columnName are present
         * @param colName to select column by columnName
         * @return true if column's type of inputted columnName is already present in columnTypes map
         */
        public boolean containsTypeForCol(String colName) {
            return _columnTypes.containsKey(colName);
        }

        /**
         * Method for incrementing count of records
         */
        public void addRecord() {
            _count += 1;
        }

        /**
         * Method for adding more byteSize to result. It can therefore be added incrementally while cycling over native result
         * @param size added size
         */
        public void addByteSize(int size) {
            _byteSize += size;
        }

        /**
         * Builder method for building instance of ConsumedResult from Builder
         * @return newly created ConsumedResult
         */
        public ConsumedResult toResult() {
            return new ConsumedResult(
                    _columnTypes,
                    _byteSize,
                    _count
            );
        }
    }

}
