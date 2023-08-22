package com.realtime.streamer.mappers.rebm;

import com.realtime.streamer.data.PsnlTag;
import com.realtime.streamer.data.Scrt;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface PsnlTagMapper {
    //�±����� ��ȸ
    List<PsnlTag> getAllPsnlTagList();
    //��ũ��Ʈ ���� ��ȸ
    List<Scrt> getPsnlScrtInfo(String actId);



}
