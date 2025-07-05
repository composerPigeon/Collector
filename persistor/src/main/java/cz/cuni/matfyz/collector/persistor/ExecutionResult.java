package cz.cuni.matfyz.collector.persistor;


import java.util.Map;

public class ExecutionResult {
    private final boolean _isSuccessful;
    private final Map<String, Object> _model;
    private final String _errorMessage;

    private ExecutionResult(Map<String, Object> model) {
        _isSuccessful = true;
        _model = model;
        _errorMessage = null;
    }

    public static ExecutionResult success(Map<String, Object> model) {
        return new ExecutionResult(model);
    }

    public static ExecutionResult error(String errorMessage) {
        return new ExecutionResult(errorMessage);
    }

    private ExecutionResult(String errorMessage) {
        _isSuccessful = false;
        _model = null;
        _errorMessage = errorMessage;
    }

    public boolean isSuccessful() {
        return _isSuccessful;
    }

    public Map<String, Object> getValue() {
        return _model;
    }

    public String getErrorMessage() {
        return _errorMessage;
    }
}
