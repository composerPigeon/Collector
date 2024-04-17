package cz.cuni.matfyz.collector.wrappers.neo4j;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractDataCollector;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import org.neo4j.driver.Result;
import org.neo4j.driver.summary.ResultSummary;

import java.util.ArrayList;
import java.util.List;

public class Neo4jDataCollector extends AbstractDataCollector<ResultSummary, Result> {

    public Neo4jDataCollector(Neo4jConnection connection, DataModel model, String datasetName) {
        super(datasetName, model, connection);
    }

    private void _savePageSize() {
        _model.datasetData().setDataSetPageSize(Neo4jResources.DefaultSizes.PAGE_SIZE);
    }


    private long _parseUnit(String unit) throws ParseException {
        if ("B".equals(unit))
            return 1;
        if ("KiB".equals(unit) || "KB".equals(unit) || "K".equals(unit) || "kB".equals(unit) || "kb".equals(unit) || "k".equals(unit))
            return 1024;
        if ("MiB".equals(unit) || "MB".equals(unit) || "M".equals(unit) || "mB".equals(unit) || "mb".equals(unit) || "m".equals(unit))
            return 1048576;
        if ("GiB".equals(unit) || "GB".equals(unit) || "G".equals(unit) || "gB".equals(unit) || "gb".equals(unit) || "g".equals(unit))
            return 1073741824;
        if ("TiB".equals(unit) || "TB".equals(unit))
            return 1099511600000L;
        if ("PiB".equals(unit) || "PB".equals(unit))
            return  1125899900000000L;
        else
            throw new ParseException("Invalid unit for memory settings: " + unit);
    }

    private int _parsePageCacheSize(String size) throws ParseException {
        StringBuilder number = new StringBuilder();
        StringBuilder unit = new StringBuilder();
        boolean isNumber = true;

        for (char ch : size.toCharArray()) {
            if (isNumber && (Character.isDigit(ch) || ch == '.'))
                number.append(ch);
            else if (isNumber && !Character.isDigit(ch)) {
                number.append(ch);
                isNumber = false;
            } else {
                unit.append(ch);
            }
        }

        return (int) (Integer.parseInt(number.toString()) * _parseUnit(unit.toString()));
    }
    private void _saveCacheSize() throws QueryExecutionException, ParseException {
        CachedResult result = _connection.executeQuery(Neo4jResources.getPageCacheSizeQuery());
        if (result.next()) {
            String stringSize = result.getString("value");
            if (!"No Value".equals(stringSize)) {
                _model.datasetData().setDataSetCacheSize(_parsePageCacheSize(stringSize));
            }
        }
    }
    private void _saveDatasetSize() throws QueryExecutionException {
        int[] nodeTuple = _fetchNodePropertiesSize(Neo4jResources.getAllNodesQuery());
        int[] edgeTuple = _fetchEdgePropertiesSize(Neo4jResources.getAllRelationsQuery());
        int size = nodeTuple[0] + edgeTuple[0];
        _model.datasetData().setDataSetSize(size);
        _model.datasetData().setDataSetSizeInPages((int) Math.ceil(
                (double) size / Neo4jResources.DefaultSizes.PAGE_SIZE
        ));
    }

    private void _saveDatasetData() throws QueryExecutionException, ParseException {
        _savePageSize();
        _saveDatasetSize();
        _saveCacheSize();
    }

    private void _saveSpecificColumnData(String label, String property, boolean isNode) throws QueryExecutionException {
        CachedResult result = isNode ? _connection.executeQuery(Neo4jResources.getNodePropertyTypeAndMandatoryQuery(label, property)) :
                _connection.executeQuery(Neo4jResources.getEdgePropertyTypeAndMandatoryQuery(label, property));
        if (result.next()) {
            boolean mandatory = result.getBoolean("mandatory");
            String type = result.getList("propertyTypes", new String[]{}).get(0);
            if ("List".equals(type) || "String".equals(type)) {
                _model.datasetData().setColumnByteSize(label, property, Neo4jResources.DefaultSizes.BIG_PROPERTY_SIZE);
            } else {
                _model.datasetData().setColumnByteSize(label, property, Neo4jResources.DefaultSizes.SMALL_PROPERTY_SIZE);
            }
            _model.datasetData().setColumnType(label, property, type);
            _model.datasetData().setColumnMandatory(label, property, mandatory);
        }
    }
    private List<String> _getPropertyNames(String query) throws QueryExecutionException {
        List<String> properties = new ArrayList<>();
        CachedResult result = _connection.executeQuery(query);
        while (result.next()) {
            String property = result.getString("propertyName");
            if (property != null) {
                properties.add(property);
            }
        }
        return properties;
    }
    private void _saveColumnData(String tableName, boolean isNode) throws QueryExecutionException {
        List<String> properties = isNode ? _getPropertyNames(Neo4jResources.getNodePropertiesForLabelQuery(tableName)) :
                _getPropertyNames(Neo4jResources.getEdgePropertiesForLabelQuery(tableName));

        for (String columnName : properties) {
            _saveSpecificColumnData(tableName, columnName, isNode);
        }
    }


    private int[] _fetchNodePropertiesSize(String fetchQuery) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(fetchQuery);
        int propertiesSize = 0;
        while (result.next()) {
            var record = result.getRecord();
            for (var column : record.entrySet()) {
                propertiesSize += Neo4jResources.DefaultSizes.getAvgColumnSizeByValue(column.getValue());
            }
        }
        return new int[]{
                result.getRowCount() * Neo4jResources.DefaultSizes.NODE_SIZE + propertiesSize, result.getRowCount()
        };
    }

    private int[] _fetchEdgePropertiesSize(String fetchQuery) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(fetchQuery);
        int propertiesSize = 0;
        while (result.next()) {
            var record = result.getRecord();
            for (var column : record.entrySet()) {
                propertiesSize += Neo4jResources.DefaultSizes.getAvgColumnSizeByValue(column.getValue());
            }
        }
        return new int[]{
                result.getRowCount() * Neo4jResources.DefaultSizes.EDGE_SIZE + propertiesSize, result.getRowCount()
        };
    }
    private void _saveTableConstraintCount(String label) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(Neo4jResources.getConstraintCountForLabelQuery(label));
        if (result.next()) {
            long count = result.getLong("count");
            _model.datasetData().setTableConstraintCount(label, count);
        }
    }

    private void _saveTableSizes(String tableName, boolean isNode) throws QueryExecutionException {
        int[] tuple = isNode ? _fetchNodePropertiesSize(Neo4jResources.getNodesOfSpecificLabelQuery(tableName)) : _fetchEdgePropertiesSize(Neo4jResources.getEdgesOfSpecificLabelQuery(tableName));
        int size = tuple[0];
        int rowCount = tuple[1];

        _model.datasetData().setTableByteSize(tableName, size);
        _model.datasetData().setTableSizeInPages(tableName, (int) Math.ceil(
                (double) size / Neo4jResources.DefaultSizes.PAGE_SIZE
        ));
        _model.datasetData().setTableRowCount(tableName, rowCount);
    }
    private boolean _isNodeLabel(String label) throws QueryExecutionException, DataCollectException {
        CachedResult result = _connection.executeQuery(Neo4jResources.getIsNodeLabelQuery(label));
        if (result.next()) {
            return result.getBoolean("isNodeLabel");
        }
        throw new DataCollectException("Invalid result of isNodeLabel query");
    }
    private void _saveTableData() throws QueryExecutionException, DataCollectException {
        for (String label : _model.getTableNames()) {
            boolean isNode = _isNodeLabel(label);
            _saveTableConstraintCount(label);
            _saveTableSizes(label, isNode);
            _saveColumnData(label, isNode);
        }
    }

    private void _saveTableNameFor(String[] indexNames) {
        _model.datasetData().addTable(indexNames[1]);
    }

    private void _saveIndexSizes(String indexName, String[] indexNames, boolean isNode) throws QueryExecutionException{
        int[] index = isNode ? _fetchNodePropertiesSize(Neo4jResources.getNodeAndPropertyQuery(indexNames[1], indexNames[2])) :
                _fetchEdgePropertiesSize(Neo4jResources.getEdgeAndPropertyQuery(indexNames[1], indexNames[2]));
        int size = (int) Math.ceil((double) (index[0]) / 3);
        int rowCount = index[1];

        _model.datasetData().setIndexRowCount(indexName, rowCount);
        _model.datasetData().setIndexByteSize(indexName, size);
        _model.datasetData().setIndexSizeInPages(indexName,
                (int) Math.ceil((double) size / Neo4jResources.DefaultSizes.PAGE_SIZE)
        );
    }

    private void _saveIndexData() throws QueryExecutionException, DataCollectException {
        for (String inxName : _model.getIndexNames()) {
            String[] names = inxName.split(":");

            boolean isNode = _isNodeLabel(names[1]);
            _saveTableNameFor(names);
            _saveIndexSizes(inxName, names, isNode);
        }
    }

    private void _saveResultData(CachedResult result) {
        long size = result.getByteSize();
        _model.resultData().setByteSize(size);

        long count = result.getRowCount();
        _model.resultData().setRowCount(count);

        long sizeInPages = (long) Math.ceil((double) size / Neo4jResources.DefaultSizes.PAGE_SIZE);
        _model.resultData().setSizeInPages(sizeInPages);

        for (String colName : result.getColumnNames()) {
            String type = result.getColumnType(colName);
            _model.resultData().setColumnType(colName, type);
            _model.resultData().setColumnByteSize(colName, Neo4jResources.DefaultSizes.getAvgColumnSizeByType(type));
        }
    }
    @Override
    public DataModel collectData(CachedResult result) throws DataCollectException {
        try {
            _saveDatasetData();
            _saveIndexData();
            _saveTableData();
            _saveResultData(result);
            return _model;
        } catch (QueryExecutionException | ParseException e) {
            throw new DataCollectException(e);
        }
    }
}
