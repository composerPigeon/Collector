package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class for saving statistical data about result
 */
public class ResultData {

    /** Field containing execution time in millis */
    @JsonProperty("executionTime")
    private Double _executionTime;

    @JsonProperty("resultKind")
    private final KindData _resultKind;

    public ResultData() {
        _resultKind = new KindData("resultKind");
        _executionTime = null;
    }

    public void setExecutionTime(double time) {
        if (_executionTime == null) _executionTime = time;
    }

    public KindData getResultKind() { return _resultKind; }
}
