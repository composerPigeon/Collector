package cz.cuni.matfyz.collector.wrappers.mongodb;

import org.bson.Document;

import javax.print.Doc;

public class MongoResources {
    public static final String DATABASE_NAME = "MongoDB";

    public static Document getExplainCommand(Document command) {
        Document newCommand = new Document();
        newCommand.put("explain", command);
        newCommand.put("verbosity", "executionStats");
        return newCommand;
    }
    public static Document getCollectionStatsCommand(String collectionName) {
        return new Document("collStats", collectionName);
    }
    public static Document getCollectionWithIndexesStatsCommand(String collectionName) {
        Document command = new Document();
        command.put("collStats", collectionName);
        command.put("indexDetails",  true);
        return command;
    }
    public static Document getIndexRowCountCommand(String collectionName, String indexName) {
        Document findCommand = new Document();
        findCommand.put("find", collectionName);
        findCommand.put("hint", indexName);

        Document countCommand = new Document();
        countCommand.put("count", collectionName);
        countCommand.put("query", findCommand);
        return countCommand;
    }
    public static Document getDatasetStatsCommand() {
        return new Document("dbStats", 1);
    }

    public static String getConnectionLink(String host, int port, String user, String password) {
        if (user.isEmpty() || password.isEmpty())
            return "mongodb://" + host + ':' + port;
        else
            return "mongodb://" + user + ':' + password + '@' + host + ':' + port;
    }
}
