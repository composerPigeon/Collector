package cz.cuni.matfyz.collector.wrappers.abstractwrapper.components;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperExceptionsFactory;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperUnsupportedOperationException;

public abstract class AbstractExplainPlanParser<TPlan> {
    protected final WrapperExceptionsFactory _exceptionsFactory;

    public AbstractExplainPlanParser(WrapperExceptionsFactory exceptionsFactory) {
        _exceptionsFactory = exceptionsFactory;
    }

    public abstract void parsePlan(TPlan plan, DataModel model) throws ParseException, WrapperUnsupportedOperationException;
}
