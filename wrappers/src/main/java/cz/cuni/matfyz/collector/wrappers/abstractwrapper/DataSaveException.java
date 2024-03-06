package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

public class DataSaveException extends WrapperException {
    public DataSaveException(Throwable cause) { super(cause); }
    public DataSaveException(String message) { super(message); }
    public DataSaveException(String message, Throwable cause) { super(message, cause); }
}
