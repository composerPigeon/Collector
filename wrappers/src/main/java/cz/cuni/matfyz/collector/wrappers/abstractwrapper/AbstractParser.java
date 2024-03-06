package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;

public abstract class AbstractParser<P> {
    protected String _datasetName;
    public AbstractParser(String datasetName) {
        _datasetName = datasetName;
    }
    public abstract DataModel parseExplainTree(String forQuery, P explainTree) throws ExplainParseException;
}
