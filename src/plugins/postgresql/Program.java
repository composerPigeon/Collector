package src.plugins.postgresql;

import src.plugins.postgresql.Executor;

class Program {

    public static void main(String[] args) {
        Executor e = new Executor("jdbc:postgresql://localhost:5432/josefholubec");

        e.executeQuery("select * from person");
    }
}