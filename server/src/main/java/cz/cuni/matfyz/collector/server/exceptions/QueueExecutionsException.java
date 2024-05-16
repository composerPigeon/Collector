package cz.cuni.matfyz.collector.server.exceptions;

public class QueueExecutionsException extends Exception {
    public QueueExecutionsException(Throwable cause) { super(cause);}
    public QueueExecutionsException(String message) { super(message);}
    public QueueExecutionsException(String message, Throwable cause) { super(message, cause);}
}
