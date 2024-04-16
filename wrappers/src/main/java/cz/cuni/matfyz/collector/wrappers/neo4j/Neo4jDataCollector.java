package cz.cuni.matfyz.collector.wrappers.neo4j;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractDataCollector;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import org.neo4j.driver.Result;
import org.neo4j.driver.summary.Plan;

public class Neo4jDataCollector extends AbstractDataCollector<Plan, Result> {

    public Neo4jDataCollector(Neo4jConnection connection, DataModel model, String datasetName) {
        super(datasetName, model, connection);
    }

    private void _savePageSize() {
        _model.toDatasetData().setDataSetPageSize(Neo4jResources.DefaultSizes.PAGE_SIZE);
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
                _model.toDatasetData().setDataSetCacheSize(_parsePageCacheSize(stringSize));
            }
        }
    }
    private void _saveDatasetSize() throws QueryExecutionException {
        int[] nodeTuple = _fetchPropertiesSize(Neo4jResources.DefaultSizes.NODE_SIZE, Neo4jResources.getAllNodesQuery());
        int[] edgeTuple = _fetchPropertiesSize(Neo4jResources.DefaultSizes.EDGE_SIZE, Neo4jResources.getAllRelationsQuery());
        int size = nodeTuple[0] + edgeTuple[0];
        _model.toDatasetData().setDataSetSize(size);
        _model.toDatasetData().setDataSetSizeInPages((int) Math.ceil(
                (double) size / Neo4jResources.DefaultSizes.PAGE_SIZE
        ));
    }

    private void _saveDatasetData() throws QueryExecutionException, ParseException {
        _savePageSize();
        _saveDatasetSize();
        _saveCacheSize();
    }

    private void _saveColumnData(String TableName) {

    }


    private int[] _fetchPropertiesSize(int recordSize, String fetchQuery) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(fetchQuery);
        int propertiesSize = 0;
        while (result.next()) {
            var record = result.getRecord();
            for (var column : record.entrySet()) {
                propertiesSize += Neo4jResources.DefaultSizes.getAvgColumnSize(column.getValue());
            }
        }
        return new int[]{
                result.getRowCount() * recordSize + propertiesSize, result.getRowCount()
        };
    }

    private void _saveTableConstraintCount(String label) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(Neo4jResources.getConstraintCountForLabelQuery(label));
        if (result.next()) {
            int count = result.getInt("count");
            _model.toDatasetData().setTableConstraintCount(label, count);
        }
    }

    private void _saveTableSizes(String tableName) throws QueryExecutionException {
        int[] nodeTuple = _fetchPropertiesSize(Neo4jResources.DefaultSizes.NODE_SIZE, Neo4jResources.getNodesQuery(tableName));
        int[] edgeTuple = _fetchPropertiesSize(Neo4jResources.DefaultSizes.EDGE_SIZE, Neo4jResources.getRelationsQuery(tableName));
        int size = nodeTuple[0] + edgeTuple[0];

        _model.toDatasetData().setTableByteSize(tableName, size);
        _model.toDatasetData().setTableSizeInPages(tableName, (int) Math.ceil(
                (double) size / Neo4jResources.DefaultSizes.PAGE_SIZE
        ));
        _model.toDatasetData().setTableRowCount(tableName, nodeTuple[1] + edgeTuple[1]);
    }
    private void _saveTableData() throws QueryExecutionException {
        for (String label : _model.getTableNames()) {
            _saveTableConstraintCount(label);
            _saveTableSizes(label);
            _saveColumnData(label);
        }
    }

    private void _saveTableNameFor(String[] indexNames) {
        _model.toDatasetData().addTable(indexNames[1]);
    }

    private void _saveIndexSizes(String indexName, String[] indexNames) throws QueryExecutionException{
        int[] nodeIndex = _fetchPropertiesSize(Neo4jResources.DefaultSizes.NODE_SIZE, Neo4jResources.getNodeAndPropertyQuery(indexNames[1], indexNames[2]));
        int[] edgeIndex = _fetchPropertiesSize(Neo4jResources.DefaultSizes.EDGE_SIZE, Neo4jResources.getEdgeAndPropertyQuery(indexNames[1], indexNames[2]));
        int size = (int) Math.ceil((double) (nodeIndex[0] + edgeIndex[0]) / 3);

        _model.toDatasetData().setIndexRowCount(indexName, nodeIndex[1] + edgeIndex[1]);
        _model.toDatasetData().setIndexByteSize(indexName, size);
        _model.toDatasetData().setIndexSizeInPages(indexName,
                (int) Math.ceil((double) size / Neo4jResources.DefaultSizes.PAGE_SIZE)
        );
    }

    private void _saveIndexData() throws QueryExecutionException {
        for (String inxName : _model.getIndexNames()) {
            String[] names = inxName.split(":");
            _saveTableNameFor(names);
            _saveIndexSizes(inxName, names);
        }
    }
    @Override
    public DataModel collectData(CachedResult result) throws DataCollectException {
        try {
            _saveDatasetData();
            _saveIndexData();
            _saveTableData();

            return _model;
        } catch (QueryExecutionException | ParseException e) {
            throw new DataCollectException(e);
        }
    }
}
