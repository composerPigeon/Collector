package cz.cuni.matfyz.collector.wrappers.queryresult;

import java.util.*;

/**
 * Class which represents ConsumedResult and provides it unified API to get measured statistical data
 */
public class ConsumedResult {

    /** Field containing all columnTypes for all columns of result */
    private final Map<String, Map<String, Integer>> _attributeTypes;

    /** Field holding byte size of result measured in bytes */
    private final long _byteSize;

    /** Field containing record count from result */
    private final long _count;

    private ConsumedResult(Map<String, Map<String, Integer>> attributeTypes, long byteSize, long count) {
        _byteSize = byteSize;
        _count = count;
        _attributeTypes = attributeTypes;
    }

    /**
     * Method for getting all column names present in result
     * @return set of the column names
     */
    public Set<String> getAttributeNames() {
        return _attributeTypes.keySet();
    }

    /**
     * Method for getting type for specific column
     * @param attributeName to select column by columnName
     * @return type of this column as string
     */
    public Iterable<String> getAttributeTypes(String attributeName) {
        return _attributeTypes.get(attributeName).keySet();
    }

    public double getAttributeTypeRatio(String attributeName, String type) {
        return (double) _attributeTypes.get(attributeName).get(type) / (double)_count;
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
    public long getRecordCount() {
        return _count;
    }
    /**
     * Class representing Builder for ConsumedResult
     */
    public static class Builder {

        private final Map<String, Map<String, Integer>> _attributeTypes;
        private long _byteSize;
        private long _count;

        public Builder() {
            _attributeTypes = new HashMap<>();
            _count = 0;
            _byteSize = 0;
        }

        private void addAttributeType(Map<String, Integer> counts, String type) {
            counts.compute(type, (k, v) -> v == null ? 1 : v + 1);
        }

        /**
         * Method for adding type for specific column
         * @param attributeName to specify column by columnName
         * @param type inputted type
         */
        public void addAttributeType(String attributeName, String type) {
            if (_attributeTypes.containsKey(attributeName)) {
                var counts = _attributeTypes.get(attributeName);
                addAttributeType(counts, type);
            } else {
                var counts = new HashMap<String, Integer>();
                addAttributeType(counts, type);
                _attributeTypes.put(attributeName, counts);
            }
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
                    _attributeTypes,
                    _byteSize,
                    _count
            );
        }
    }

}
