package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.PsnlTag;
import com.realtime.streamer.mappers.rebm.PsnlTagMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.List;
@Repository
@RequiredArgsConstructor
public class MyBatisPsnlTagRepository implements PsnlTagRepository{

    private  final PsnlTagMapper psnlTagMapper;

    @Override
    public List<PsnlTag> getAllPsnlTagList() {
        return psnlTagMapper.getAllPsnlTagList();
    }
}
