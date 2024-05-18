package cz.cuni.matfyz.collector.wrappers.mongodb.queryparser;

/**
 * Class which represents called mongosh function used in query
 */
public class FunctionItem {
    public final String name;
    public final ArgumentsArray args;

    public FunctionItem(String name, ArgumentsArray args) {
        this.name = name;
        this.args = args;
    }
}
