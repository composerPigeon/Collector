package cz.cuni.matfyz.collector.wrappers.neo4j;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractDataCollector;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.cachedresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import org.neo4j.driver.Result;
import org.neo4j.driver.summary.ResultSummary;

import java.util.ArrayList;
import java.util.List;

public class Neo4jDataCollector extends AbstractDataCollector<ResultSummary, Result, String> {

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
        long[] nodeTuple = _fetchNodePropertiesSize(Neo4jResources.getAllNodesQuery());
        long[] edgeTuple = _fetchEdgePropertiesSize(Neo4jResources.getAllRelationsQuery());
        long size = nodeTuple[0] + edgeTuple[0];
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
            String type = result.getList("propertyTypes", String.class).get(0);
            int columnSize = Neo4jResources.DefaultSizes.getAvgColumnSizeByType(type);
            _model.datasetData().setColumnByteSize(label, property, columnSize);
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


    private long[] _fetchNodePropertiesSize(String fetchQuery) throws QueryExecutionException {
        ConsumedResult result = _connection.executeQueryAndConsume(fetchQuery);
        return new long[]{
                result.getRowCount() * Neo4jResources.DefaultSizes.NODE_SIZE + result.getByteSize(), result.getRowCount()
        };
    }

    private long[] _fetchEdgePropertiesSize(String fetchQuery) throws QueryExecutionException {
        ConsumedResult result = _connection.executeQueryAndConsume(fetchQuery);
        return new long[]{
                result.getRowCount() * Neo4jResources.DefaultSizes.EDGE_SIZE + result.getByteSize(), result.getRowCount()
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
        long[] tuple = isNode ? _fetchNodePropertiesSize(Neo4jResources.getNodesOfSpecificLabelQuery(tableName)) : _fetchEdgePropertiesSize(Neo4jResources.getEdgesOfSpecificLabelQuery(tableName));
        long size = tuple[0];
        long rowCount = tuple[1];

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
        long[] index = isNode ? _fetchNodePropertiesSize(Neo4jResources.getNodeAndPropertyQuery(indexNames[1], indexNames[2])) :
                _fetchEdgePropertiesSize(Neo4jResources.getEdgeAndPropertyQuery(indexNames[1], indexNames[2]));
        long size = (long) Math.ceil((double) (index[0]) / 3);
        long rowCount = index[1];

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

    private void _saveResultData(ConsumedResult result) {
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
    public DataModel collectData(ConsumedResult result) throws DataCollectException {
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
