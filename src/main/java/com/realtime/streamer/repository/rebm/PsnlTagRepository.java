package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.PsnlTag;
import com.realtime.streamer.data.Scrt;

import java.util.List;
import java.util.Map;

public interface PsnlTagRepository{
    List<PsnlTag> getAllPsnlTagList();
    List<Scrt> getPsnlScrtInfo(String actId);
    List<Map<String, Object>> getPsnlScrtColInfo(String dbPool, String qry, Object[] objArr);
    List<Scrt> getH2PsnlScrtInfo(String actId);
}
