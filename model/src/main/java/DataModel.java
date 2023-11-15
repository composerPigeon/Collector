package model.src.main.java;

public class DataModel {

    //Gathered data (objects that will be trasnlated to JSON using GSON)
    private String dbName;
    private String database;

    private QueryData beforeQueryData;
    private QueryData afterQueryData;

    public DataModel(String database, String dbName) {
        this.dbName = dbName;
        this.database = database;

        beforeQueryData = new QueryData();
        afterQueryData = new QueryData();
    }

    public QueryData beforeQuery() {
        return beforeQueryData;
    }

    public QueryData afterQuery() {
        return afterQueryData;
    }


    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        
        str.append(database + ":\n");
        str.append("Database name: " + dbName + "\n");
        str.append("DataBeforeQuery:\n");
        str.append(beforeQueryData.toString(1));
        str.append("\nDataAfterQuery:\n");
        str.append(afterQueryData.toString(1));

        return str.toString();
    }
}
