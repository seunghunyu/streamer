package com.realtime.streamer.repository.rebm;

import java.util.List;
import java.util.Map;

public interface DataLoadRepository {
    public void dataLoad();
    public void createTable(String qry);
    public void createIndex(String qry);
    public void insertData(String qry, Map<String, Object> dataMap);
    public List<?> selectData(String qry, String dbPool);
}
