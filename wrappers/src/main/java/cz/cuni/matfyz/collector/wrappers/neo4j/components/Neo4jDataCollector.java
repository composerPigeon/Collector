package cz.cuni.matfyz.collector.wrappers.neo4j.components;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.AbstractDataCollector;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.AbstractQueryResultParser;
import cz.cuni.matfyz.collector.wrappers.exceptions.ConnectionException;
import cz.cuni.matfyz.collector.wrappers.neo4j.Neo4jResources;
import cz.cuni.matfyz.collector.wrappers.queryresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.queryresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import org.neo4j.driver.Result;
import org.neo4j.driver.summary.ResultSummary;

import java.math.BigDecimal;
import java.util.*;

/**
 * Class responsible for collecting all statistical data from neo4j
 */
public class Neo4jDataCollector extends AbstractDataCollector<Result, String, ResultSummary> {

    public Neo4jDataCollector(
            AbstractWrapper.ExecutionContext<Result, String, ResultSummary> context,
            AbstractQueryResultParser<Result> resultParser
    ) throws ConnectionException {
        super(context, resultParser);
    }

    /**
     * Method which saves page size used by databases storage engine
     */
    private void _collectPageSize() {
        getModel().setPageSize(Neo4jResources.DefaultSizes.PAGE_SIZE);
    }

    /**
     * Method responsible for parsing unit which is returned with cache size
     * @param unit string representation of unit
     * @return multiplier which corresponds to specified unit
     * @throws DataCollectException when unit cannot be parsed
     */
    private long _parseUnit(String unit) throws DataCollectException {
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
            //TODO: update
            throw new DataCollectException("Invalid unit for memory settings: " + unit);
    }

    /**
     * Method responsible for parsing caching size
     * @param size string which contains number and unit
     * @return parsed number which is size in bytes
     * @throws DataCollectException when input cannot be parsed
     */
    private long _parsePageCacheSize(String size) throws DataCollectException {
        StringBuilder number = new StringBuilder();
        StringBuilder unit = new StringBuilder();
        boolean isNumber = true;

        for (char ch : size.toCharArray()) {
            if (isNumber && (Character.isDigit(ch) || ch == '.'))
                number.append(ch);
            else if (isNumber && !Character.isDigit(ch)) {
                unit.append(ch);
                isNumber = false;
            } else {
                unit.append(ch);
            }
        }

        return (new BigDecimal(number.toString()).longValue() * _parseUnit(unit.toString()));
    }

    /**
     * Method which saves the gathered cache size to data model
     * @throws DataCollectException when QueryExecutionException occur in help query evaluation
     */
    private void _collectCacheSize() throws DataCollectException {
        CachedResult result = executeQuery(Neo4jResources.getPageCacheSizeQuery());
        if (result.next()) {
            String stringSize = result.getString("value");
            if (!"No Value".equals(stringSize)) {
                getModel().setDatabaseCacheSize(_parsePageCacheSize(stringSize));
            }
        }
    }

    /**
     * Method responsible for saving all dataset sizes parameters to data model
     * @throws DataCollectException when it is thrown from some of the help queries
     */
    private void _collectDatabaseSize() throws DataCollectException {
        CachedResult result = executeQuery(Neo4jResources.getDatabaseSizesQuery());
        if (result.next()) {
            long size = result.getLong("totalStoreSize");
            getModel().setDatabaseByteSize(size);
            getModel().setDatabaseSizeInPages((int) Math.ceil(
                    (double) size / Neo4jResources.DefaultSizes.PAGE_SIZE
            ));
        }
    }

    /**
     * Method which saves all dataset data
     * @throws DataCollectException whe some of the help queries produced a problem
     */
    private void _collectDatabaseData() throws DataCollectException {
        _collectPageSize();
        _collectDatabaseSize();
        _collectCacheSize();
    }

    // Save ColumnData

    /**
     * Method responsible for saving all column data
     * @param labelSizeData is data fetched from iterating through all entities of specific label
     * @param property is field for which data we are interested in
     * @throws DataCollectException when some of the help queries fail
     */
    private void _collectSpecificPropertyData(String property, LabelSizeData labelSizeData) throws DataCollectException {
        CachedResult result = labelSizeData.isNode()
                ? executeQuery(Neo4jResources.getNodePropertyTypeAndMandatoryQuery(labelSizeData.getLabel(), property))
                : executeQuery(Neo4jResources.getEdgePropertyTypeAndMandatoryQuery(labelSizeData.getLabel(), property));
        if (result.next()) {
            boolean mandatory = result.getBoolean("mandatory");
            getModel().setAttributeMandatory(labelSizeData.getLabel(), property, mandatory);

            for (String type : result.getList("propertyTypes", String.class)) {
                int columnSize = Neo4jResources.DefaultSizes.getAvgColumnSizeByType(type);
                getModel().setAttributeTypeByteSize(labelSizeData.getLabel(), property, type, columnSize);
                getModel().setAttributeTypeRatio(labelSizeData.getLabel(), property, type, labelSizeData.getPropertyTypeRatio(property, type));
            }
        }

        if (labelSizeData.isNode()) {
            result = executeQuery(Neo4jResources.getCountOfDistinctValuesForNodesQuery(labelSizeData.getLabel(), property));

            if (result.next()) {
                long count = result.getLong("count");
                getModel().setAttributeDistinctValuesCount(labelSizeData.getLabel(), property, count);
            }
        }

    }

    /**
     * Method responsible for getting all properties for entity of some label
     * @param query query which defines on what entities we are interested
     * @return list of names for this properties
     * @throws DataCollectException when help query fails
     */
    private List<String> _getPropertyNames(String query) throws DataCollectException {
        List<String> properties = new ArrayList<>();
        CachedResult result = executeQuery(query);
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
     * @param labelSizeData is data fetched from iterating through all entities of specific label
     * @throws DataCollectException when some of the help queries fails
     */
    private void _collectPropertyData(LabelSizeData labelSizeData) throws DataCollectException {
        for (String propertyName : labelSizeData.getPropertyNames()) {
            _collectSpecificPropertyData(propertyName, labelSizeData);
        }
    }

    /**
     * Method responsible for saving how much constraints were used on entities of specified label
     * @param label specifies entities we are interested in
     * @throws DataCollectException when help query fails
     */
    private void _collectNodesOrEdgesConstraintCount(String label) throws DataCollectException {
        CachedResult result = executeQuery(Neo4jResources.getConstraintCountForLabelQuery(label));
        if (result.next()) {
            int count = result.getInt("count");
            getModel().setKindConstraintCount(label, count);
        }
    }

    /**
     * Method responsible for saving sizes of entities of specific labels
     * @param label is label of entities we are interested in
     * @param isNode indicates if we are interested in nodes or edges
     * @throws DataCollectException when some of the help queries fails
     */
    private LabelSizeData _collectNodesOrEdgesSizes(String label, boolean isNode) throws DataCollectException {
        LabelSizeData sizes = LabelSizeData.fetchEntitiesSizesData(label, isNode, this::executeQueryAndConsume);
        long size = sizes.getByteSize();
        long recordCount = sizes.getCount();

        getModel().setKindByteSize(label, size);
        getModel().setKindSizeInPages(label, (int) Math.ceil(
                (double) size / Neo4jResources.DefaultSizes.PAGE_SIZE
        ));
        getModel().setKindRecordCount(label, recordCount);

        return sizes;
    }

    /**
     * Method which is responsible deciding if entity of specified label is node or edge
     * @param label specified label
     * @return true if label of entity is node label
     * @throws DataCollectException when result of help query is empty
     */
    private boolean _isNodeLabel(String label) throws DataCollectException {
        CachedResult result = executeQuery(Neo4jResources.getIsNodeLabelQuery(label));
        if (result.next()) {
            return result.getBoolean("isNodeLabel");
        }
        throw new DataCollectException("Invalid result of isNodeLabel query");
    }

    /**
     * Method which saves all data for entities used by query
     * @throws DataCollectException when invalid label is used
     */
    private void _collectNodesAndEdgesData() throws DataCollectException {
        for (String label : getModel().getKindNames()) {
            boolean isNode = _isNodeLabel(label);
            _collectNodesOrEdgesConstraintCount(label);
            LabelSizeData labelSizeData = _collectNodesOrEdgesSizes(label, isNode);
            _collectPropertyData(labelSizeData);
        }
    }

    /**
     * Method which is responsible for saving index sizes specified by index name over entities
     * @param indexRecord identifier of index
     * @param isNode indicates whether the entity is node or edge
     * @throws DataCollectException when some of the help queries will fail
     */
    private void _collectIndexSizes(IndexParseRecord indexRecord, boolean isNode) throws DataCollectException {
        LabelSizeData sizes = LabelSizeData.fetchIndexSizesData(
                indexRecord.getLabel(),
                isNode,
                indexRecord.getProperties(),
                this::executeQueryAndConsume
        );
        long size = (long) Math.ceil((double) (sizes.getByteSize()) / 3);
        long recordCount = sizes.getCount();

        getModel().setIndexRecordCount(indexRecord.getIndexName(), recordCount);
        getModel().setIndexByteSize(indexRecord.getIndexName(), size);
        getModel().setIndexSizeInPages(indexRecord.getIndexName(),
                (int) Math.ceil((double) size / Neo4jResources.DefaultSizes.PAGE_SIZE)
        );
    }

    /**
     * Method for saving all data about used indexes
     * @throws DataCollectException when some of the labels are invalid
     */
    private void _collectIndexData() throws DataCollectException {
        for (String inxName : getModel().getIndexNames()) {
            IndexParseRecord indexRecord = new IndexParseRecord(inxName);

            getModel().addKind(indexRecord.getLabel());
            boolean isNode = _isNodeLabel(indexRecord.getLabel());
            _collectIndexSizes(indexRecord, isNode);
        }
    }

    /**
     * Method which is saving all statistics about result data to dataModel
     * @param result result of main query
     */
    private void _collectResultData(ConsumedResult result) {
        long size = result.getByteSize();
        getModel().setResultByteSize(size);

        long count = result.getRecordCount();
        getModel().setResultRecordCount(count);

        long sizeInPages = (long) Math.ceil((double) size / Neo4jResources.DefaultSizes.PAGE_SIZE);
        getModel().setResultSizeInPages(sizeInPages);

        for (String colName : result.getAttributeNames()) {
            for (String type : result.getAttributeTypes(colName)) {
                getModel().setResultAttributeTypeByteSize(colName, type, Neo4jResources.DefaultSizes.getAvgColumnSizeByType(type));
                double ratio = result.getAttributeTypeRatio(colName, type);
                getModel().setResultAttributeTypeRatio(colName, type, ratio);
            }
        }
    }

    /**
     * Public method which triggers collecting of all statistical data and returns result as instance of DataModel
     * @param result main query result
     * @throws DataCollectException when some of the help queries fails or invalid labels were used
     */
    @Override
    public void collectData(ConsumedResult result) throws DataCollectException {
        _collectDatabaseData();
        _collectIndexData();
        _collectNodesAndEdgesData();
        _collectResultData(result);
    }

    private static class LabelSizeData {
        private final String _label;
        private final boolean _isNode;
        private final long _byteSize;
        private final long _count;
        private final Map<String, Map<String, Double>> _properties;

        public static LabelSizeData fetchEntitiesSizesData(String label, boolean isNode, ExecuteQueryAndConsume executeQueryAndConsume) throws DataCollectException {
             ConsumedResult result = executeQueryAndConsume.apply(isNode
                     ? Neo4jResources.getNodesOfSpecificLabelQuery(label)
                     : Neo4jResources.getEdgesOfSpecificLabelQuery(label)
             );

             return new LabelSizeData(label, isNode, result);
        }

        public static LabelSizeData fetchIndexSizesData(String label, boolean isNode, String[] properties, ExecuteQueryAndConsume executeQueryAndConsume) throws DataCollectException {
            ConsumedResult result = executeQueryAndConsume.apply(isNode
                    ? Neo4jResources.getNodesWithProjectionQuery(label, properties)
                    : Neo4jResources.getEdgesWithProjectionQuery(label, properties)
            );

            return new LabelSizeData(label, isNode, result);
        }

        private LabelSizeData(String label, boolean isNode, ConsumedResult result) {
            _label = label;
            _isNode = isNode;
            _count = result.getRecordCount();
            _properties = new HashMap<>();

            int recordSize = isNode ? Neo4jResources.DefaultSizes.NODE_SIZE : Neo4jResources.DefaultSizes.EDGE_SIZE;
            _byteSize = result.getRecordCount() * recordSize + result.getByteSize();

            for (String property : result.getAttributeNames()) {
                _properties.put(property, new HashMap<>());
                for (String type : result.getAttributeTypes(property)) {
                    _properties.get(property).put(type, result.getAttributeTypeRatio(property, type));
                }
            }
        }

        public String getLabel() {return _label;}

        public boolean isNode() {return _isNode;}

        public long getByteSize() {
            return _byteSize;
        }

        public long getCount() {
            return _count;
        }

        public Set<String> getPropertyNames() {
            return _properties.keySet();
        }

        public double getPropertyTypeRatio(String property, String type) {
            return _properties.get(property).get(type);
        }
    }

    @FunctionalInterface
    private interface ExecuteQueryAndConsume {
        ConsumedResult apply(String query) throws DataCollectException;
    }
}
