package cz.cuni.matfyz.collector.wrappers.abstractwrapper.components;

import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperExceptionsFactory;
import cz.cuni.matfyz.collector.wrappers.queryresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.queryresult.ConsumedResult;

public abstract class AbstractQueryResultParser<TResult> extends AbstractComponent {

    public AbstractQueryResultParser(WrapperExceptionsFactory exceptionsFactory) {
        super(exceptionsFactory);
    }

    public abstract CachedResult parseResultAndCache(TResult result) throws ParseException;

    public abstract ConsumedResult parseResultAndConsume(TResult result) throws ParseException;

}
