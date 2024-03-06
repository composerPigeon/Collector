package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

public abstract class AbstractConnection<P, R> implements AutoCloseable {
    protected P _mainPlan;
    protected R _mainResult;
    protected String _lastQuery;

    public abstract void executeMainQuery(String query) throws QueryExecutionException;
    public R getMainQueryResult() { return _mainResult; }
    public P getExplainTree() {
        return _mainPlan;
    }
    public abstract R executeQuery(String query) throws QueryExecutionException;
}
