package com.realtime.streamer.service;

import com.realtime.streamer.data.PsnlTag;
import com.realtime.streamer.data.Scrt;
import com.realtime.streamer.repository.rebm.MyBatisPsnlTagRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class PsnlTagService {
    @Autowired
    MyBatisPsnlTagRepository psnlTagRepository;

    public List<PsnlTag> getAllPsnlTagList(){
        return psnlTagRepository.getAllPsnlTagList();
    }

    public List<Scrt> getPsnlScrtInfo(String actId){
        return psnlTagRepository.getPsnlScrtInfo(actId);
    }
    public List<Map<String, Object>> getPsnlScrtColInfo(String dbPool, String qry, Object[] objArr) {
        return psnlTagRepository.getPsnlScrtColInfo(dbPool, qry, objArr);
    }

    public List<Scrt> getH2PsnlScrtInfo(String actId){
        return psnlTagRepository.getH2PsnlScrtInfo(actId);
    }
}
