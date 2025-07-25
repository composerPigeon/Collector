package cz.cuni.matfyz.collector.wrappers.exceptions;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;

/**
 * Exception which encapsulates all other exceptions from this package so when they are thrown from any part of the application, they can be caught in QueryScheduler and properly handled
 */
public class WrapperException extends Exception {

    public WrapperException(String message) {
        super(message);
    }
    public WrapperException(Throwable cause) {
        super(cause);
    }
    public WrapperException(String message, Throwable cause) {
        super(message, cause);
    }
}
