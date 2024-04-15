package cz.cuni.matfyz.collector.wrappers.abstractwrapper.exceptions;

public class ParseException extends WrapperException {
    public ParseException(String message) { super(message); }
    public ParseException(Throwable cause) { super(cause); }
    public ParseException(String message, Throwable cause) { super(message, cause); }
}
