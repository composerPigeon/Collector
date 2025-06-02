package cz.cuni.matfyz.collector.wrappers.abstractwrapper.components;

import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperExceptionsFactory;

public abstract class AbstractComponent {
    private final WrapperExceptionsFactory _exceptionsFactory;

    public AbstractComponent(WrapperExceptionsFactory exceptionsFactory) {
        _exceptionsFactory = exceptionsFactory;
    }

    protected WrapperExceptionsFactory getExceptionsFactory() {
        return _exceptionsFactory;
    }

    protected <TFactory> TFactory getExceptionsFactory(Class<TFactory> factoryClass) {
        return factoryClass.cast(_exceptionsFactory);
    }
}
