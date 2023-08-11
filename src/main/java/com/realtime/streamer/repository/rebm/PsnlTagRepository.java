package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.PsnlTag;
import com.realtime.streamer.data.Scrt;

import java.util.List;

public interface PsnlTagRepository{
    List<PsnlTag> getAllPsnlTagList();
    List<Scrt> getPsnlScrtInfo(String actId);
}
