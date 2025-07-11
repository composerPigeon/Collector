package cz.cuni.matfyz.collector.wrappers.exceptions;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;

/**
 * Exception thrown from instance of AbstractParser when some error occur during parsing of explain tree or result
 */
public class ParseException extends WrapperException {
    public ParseException(String message) {
        super(message);
    }
    public ParseException(Throwable cause) {
        super(cause);
    }
    public ParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
