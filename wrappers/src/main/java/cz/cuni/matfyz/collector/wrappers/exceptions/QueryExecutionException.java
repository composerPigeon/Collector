package cz.cuni.matfyz.collector.wrappers.abstractwrapper.exceptions;

public class QueryExecutionException extends WrapperException {
    public QueryExecutionException(String message) { super(message); }
    public QueryExecutionException(Throwable cause) { super(cause); }
    public QueryExecutionException(String message, Throwable cause) { super(message, cause); }
}
