package cz.cuni.matfyz.collector.wrappers.exceptions;

public class DataCollectException extends WrapperException {
    public DataCollectException(Throwable cause) { super(cause); }
    public DataCollectException(String message) { super(message); }
    public DataCollectException(String message, Throwable cause) { super(message, cause); }
}
