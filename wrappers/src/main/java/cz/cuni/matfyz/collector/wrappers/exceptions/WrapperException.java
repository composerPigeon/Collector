package cz.cuni.matfyz.collector.wrappers.exceptions;

public class WrapperException extends Exception {
    public WrapperException(String message) { super(message); }
    public WrapperException(Throwable cause) { super(cause); }
    public WrapperException(String message, Throwable cause) { super(message, cause); }
}
