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

/**
 * Class responsible for collecting all statistical data from neo4j
 */
public class Neo4jDataCollector extends AbstractDataCollector<ResultSummary, Result, String> {

    public Neo4jDataCollector(Neo4jConnection connection, DataModel model, String datasetName) {
        super(datasetName, model, connection);
    }

    /**
     * Method which saves page size used by databases storage engine
     */
    private void _savePageSize() {
        _model.datasetData().setDataSetPageSize(Neo4jResources.DefaultSizes.PAGE_SIZE);
    }

    /**
     * Method responsible for parsing unit which is returned with cache size
     * @param unit string representation of unit
     * @return multiplier which corresponds to specified unit
     * @throws ParseException when unit cannot be parsed
     */
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

    /**
     * Method reponsible for parsing caching size
     * @param size string which contains number and unit
     * @return parsed number which is size in bytes
     * @throws ParseException when input cannot be parsed
     */
    private long _parsePageCacheSize(String size) throws ParseException {
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

        return (Integer.parseInt(number.toString()) * _parseUnit(unit.toString()));
    }

    /**
     * Method which saves the gahered cache size to data model
     * @throws QueryExecutionException when QueryExecutionException occur in help query evaluation
     * @throws ParseException when ParseException occur during unit parsing process
     */
    private void _saveCacheSize() throws QueryExecutionException, ParseException {
        CachedResult result = _connection.executeQuery(Neo4jResources.getPageCacheSizeQuery());
        if (result.next()) {
            String stringSize = result.getString("value");
            if (!"No Value".equals(stringSize)) {
                _model.datasetData().setDataSetCacheSize(_parsePageCacheSize(stringSize));
            }
        }
    }

    /**
     * Method responsible for saving all dataset sizes parameters to data model
     * @throws QueryExecutionException when it is thrown from some of the help queries
     */
    private void _saveDatasetSize() throws QueryExecutionException {
        long[] nodeTuple = _fetchNodePropertiesSize(Neo4jResources.getAllNodesQuery());
        long[] edgeTuple = _fetchEdgePropertiesSize(Neo4jResources.getAllRelationsQuery());
        long size = nodeTuple[0] + edgeTuple[0];
        _model.datasetData().setDataSetSize(size);
        _model.datasetData().setDataSetSizeInPages((int) Math.ceil(
                (double) size / Neo4jResources.DefaultSizes.PAGE_SIZE
        ));
    }

    /**
     * Method which saves all dataset data
     * @throws QueryExecutionException whe some of the help queries produced a problem
     * @throws ParseException when there was some parsing issue with parsing cacheSize
     */
    private void _saveDatasetData() throws QueryExecutionException, ParseException {
        _savePageSize();
        _saveDatasetSize();
        _saveCacheSize();
    }

    // Save ColumnData

    /**
     * Mathod responsible for saving all column data
     * @param label is label of node or edge from neo4j graoh
     * @param property is field for which data we are interested in
     * @param isNode boolean which indicates if we are parsing node property or edge property
     * @throws QueryExecutionException when some of the help queries fail
     */
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

    /**
     * Method responsible for getting all properties for entity of some label
     * @param query query which defines on what entities we are interested
     * @return list of names for this properties
     * @throws QueryExecutionException when help query fails
     */
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

    /**
     * Method responsible for saving all column data for entities of specific label
     * @param tableName is label of entities
     * @param isNode indicates if entity is node or edge
     * @throws QueryExecutionException when some of the help queries fails
     */
    private void _saveColumnData(String tableName, boolean isNode) throws QueryExecutionException {
        List<String> properties = isNode ? _getPropertyNames(Neo4jResources.getNodePropertiesForLabelQuery(tableName)) :
                _getPropertyNames(Neo4jResources.getEdgePropertiesForLabelQuery(tableName));

        for (String columnName : properties) {
            _saveSpecificColumnData(tableName, columnName, isNode);
        }
    }

    /**
     * Method which is responsible for calculating sizes of nodes of specific label
     * @param fetchQuery defines on which nodes we are interested
     * @return array of size 2, where first number is byteSize of nodes in the collection and second one is their count
     * @throws QueryExecutionException when help query fails
     */
    private long[] _fetchNodePropertiesSize(String fetchQuery) throws QueryExecutionException {
        ConsumedResult result = _connection.executeQueryAndConsume(fetchQuery);
        return new long[]{
                result.getRowCount() * Neo4jResources.DefaultSizes.NODE_SIZE + result.getByteSize(), result.getRowCount()
        };
    }

    /**
     * Method which is responsible for calculating sizes of edges of specific label
     * @param fetchQuery defines on which edges we are interested
     * @return array of size 2, where first number is byteSize of edges in the collection and second one is their count
     * @throws QueryExecutionException when help query fails
     */
    private long[] _fetchEdgePropertiesSize(String fetchQuery) throws QueryExecutionException {
        ConsumedResult result = _connection.executeQueryAndConsume(fetchQuery);
        return new long[]{
                result.getRowCount() * Neo4jResources.DefaultSizes.EDGE_SIZE + result.getByteSize(), result.getRowCount()
        };
    }

    /**
     * Method responsible for saving how much constraints were used on entities of specified label
     * @param label speciufies entities we are interested in
     * @throws QueryExecutionException when help query fails
     */
    private void _saveTableConstraintCount(String label) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(Neo4jResources.getConstraintCountForLabelQuery(label));
        if (result.next()) {
            long count = result.getLong("count");
            _model.datasetData().setTableConstraintCount(label, count);
        }
    }

    /**
     * Mathod responsible for saving sizes of entities of specific labels
     * @param tableName is label of entities we are interested in
     * @param isNode indicates if we are interested in nodes or edges
     * @throws QueryExecutionException when some of the help queries fails
     */
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

    /**
     * Method which is responsible deciding if entity of specified label is node or edge
     * @param label specified label
     * @return true if label of entity is node label
     * @throws QueryExecutionException when help query fails
     * @throws DataCollectException when result of help query is empty
     */
    private boolean _isNodeLabel(String label) throws QueryExecutionException, DataCollectException {
        CachedResult result = _connection.executeQuery(Neo4jResources.getIsNodeLabelQuery(label));
        if (result.next()) {
            return result.getBoolean("isNodeLabel");
        }
        throw new DataCollectException("Invalid result of isNodeLabel query");
    }

    /**
     * Method which saves all data for entities used by query
     * @throws QueryExecutionException when some of the help queries fails
     * @throws DataCollectException when invalid label is used
     */
    private void _saveTableData() throws QueryExecutionException, DataCollectException {
        for (String label : _model.getTableNames()) {
            boolean isNode = _isNodeLabel(label);
            _saveTableConstraintCount(label);
            _saveTableSizes(label, isNode);
            _saveColumnData(label, isNode);
        }
    }

    /**
     * Method which gets labels over which is this index build
     * @param indexNames specify index by its name
     */
    private void _saveTableNameFor(String[] indexNames) {
        _model.datasetData().addTable(indexNames[1]);
    }

    /**
     * Method which is responsible for saving index sizes specified by index name over entities
     * @param indexName identifier of index
     * @param indexNames identifier split to array for usage of concrete tokens
     * @param isNode indicates whether the entity is node or edge
     * @throws QueryExecutionException when some of the help queries will fail
     */
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

    /**
     * Method for saving all data about used indexes
     * @throws QueryExecutionException when some of the help queries fail
     * @throws DataCollectException when some of the labels are invalid
     */
    private void _saveIndexData() throws QueryExecutionException, DataCollectException {
        for (String inxName : _model.getIndexNames()) {
            String[] names = inxName.split(":");

            boolean isNode = _isNodeLabel(names[1]);
            _saveTableNameFor(names);
            _saveIndexSizes(inxName, names, isNode);
        }
    }

    /**
     * Method which is saving all statistics about result data to dataModel
     * @param result result of main query
     */
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

    /**
     * Public method which triggers collecting of all statistical data and returns result as instance of DataModel
     * @param result main query result
     * @return instance of DataModel
     * @throws DataCollectException when some of the help queries fails or invalid labels were used
     */
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
