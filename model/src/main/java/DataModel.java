package model.src.main.java;

import java.util.ArrayList;

import model.src.main.java.BeforeQueryData;
import model.src.main.java.AfterQueryData;

public class DataModel {

    //Gathered data (objects that will be trasnlated to JSON using GSON)
    BeforeQueryData beforeQueryData;
    AfterQueryData afterQueryData;
    
    //Items that we will be ineterested in after parsing query
    private ArrayList<String> affectedTables;

    public DataModel(ArrayList<String> affectedTables) {
        this.affectedTables = affectedTables;
    }

    public BeforeQueryData getBeforeQueryData() {
        return beforeQueryData;
    }

    public AfterQueryData getAfterQueryData() {
        return afterQueryData;
    }
}
