package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.cachedresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;

public abstract class AbstractParser<TPlan, TResult> {
    public abstract void parseExplainTree(DataModel model, TPlan explainTree) throws ParseException;
    public abstract CachedResult parseResult(TResult result) throws ParseException;
    public abstract ConsumedResult parseMainResult(TResult result, DataModel withModel) throws ParseException;
}
