package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.queryresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.queryresult.CachedResult;

/**
 * Class which is responsible for parsing native results my own unified and gathering data from explain tree
 * @param <TPlan> type of explain plan result
 * @param <TResult> type of result
 */
public abstract class AbstractParser<TPlan, TResult> {
    /**
     * Method for parsing explain tree. It collects important data from explain such as used table names etc.
     * @param model instance of DataModel where collected information are stored
     * @param explainTree explain tree to be parsed
     * @throws ParseException when some error occur during processing of explain tree
     */
    public abstract void parseExplainTree(DataModel model, TPlan explainTree) throws ParseException;

    /**
     * Method for parsing native result to cached one
     * @param result result of some query
     * @return it's equivqlent result of mine unified type CachedResult
     * @throws ParseException when some error occur during parsing of result
     */
    public abstract CachedResult parseResult(TResult result) throws ParseException;

    /**
     * Method for parsing main native result to consumed one. It collects statistical data about it without caching.
     * @param result is native result of some query
     * @param withModel instance of DataModel for getting important data such as tableNames, so information about result columns can be gathered
     * @return consumed result
     * @throws ParseException when some error occur during parsing of main result
     */
    public abstract ConsumedResult parseMainResult(TResult result, DataModel withModel) throws ParseException;

    /**
     * Method for parsing native result to consumed one. It collects statistical data about it without caching.
     * @param result is native result of some query
     * @return consumed result of some query
     * @throws ParseException when some problem occur during parsing process
     */
    public abstract ConsumedResult parseResultAndConsume(TResult result) throws ParseException;
}
