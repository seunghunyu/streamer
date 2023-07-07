package com.realtime.streamer.util;

import com.realtime.streamer.data.DetcChan;
import com.realtime.streamer.data.DetcChanSqlInfo;
import com.realtime.streamer.repository.JdbcTemplateCampRepository;
import com.realtime.streamer.repository.JdbcTemplateDetcChanRepository;
import com.realtime.streamer.repository.JdbcTemplateDetcChanSqlInfoRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/* 필요 메소드 정의
 * [2023.07.05] 신규생성
 */
@Component
@RequiredArgsConstructor
public class Utility {
    /*
     *  [2023.07.05] 요일별 테이블을 구하기 위한 날짜 조회
     *               기존 DayOfWeek 클래스 기준 1,2,3,4,5,6,7(월,화,수,목,금,토)
     *                               현재 기준 2,3,4,5,6,7,1(월,화,수,목,금,토) -> 0은 Simulation
     */
    @Autowired
    JdbcTemplateDetcChanRepository detcChanRepository;

    @Autowired
    JdbcTemplateDetcChanSqlInfoRepository detcChanSqlInfoRepository;

    @Autowired
    StringRedisTemplate redisTemplate;

    public String getTableDtNum() {
        LocalDateTime date = LocalDate.now().atStartOfDay();
        //System.out.println("dayofWeekNumber" + Integer.toString(dayOfWeekNumber);
        return Integer.toString(date.getDayOfWeek().getValue()-1);
    }
    /*
     *  [2023.07.05] 사용 중인 감지채널 리스트 업로드
     *               Redis Server에 업로드
     */
    public void setRedisDetcChanList(){
        String key = "dectChanList";
        if(detcChanRepository == null){
            System.out.println("detcChanRepository is null@@@@@@@");
            return;
        }
        List<DetcChan> useDetcChanList = detcChanRepository.getUseDetcChanList();

        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"+useDetcChanList);
        //1.String
//        ValueOperations<String, String> strinValueOperations = redisTemplate.opsForValue();
        //2.List
        ListOperations<String, String> stringListOperations = redisTemplate.opsForList();
        //해당 키로 value값 존재시 삭제 후 업로드
        redisTemplate.delete(key);

        for(int i=0 ; i < useDetcChanList.size() ; i++){
            //KEY 값에 REBM_DETC_LIST -> '_' 입력시 에러 발생
            stringListOperations.rightPush(key, useDetcChanList.get(i).getDetcChanCd());
        }

        System.out.println("DETC_CHAN_LIST UPLOAD COMPLETE::::::::::::::::");
//        List<String> ResultRange = stringListOperations.range(key, 0, stringListOperations.size(key));
        System.out.println(stringListOperations.range(key, 0, stringListOperations.size(key)));

    }

    /*
     *  [2023.07.05] 사용 중인 감지채널 리스트 Redis Server에서 가져오기
     */
    public String getRedisDetcChanList(){
        String key = "dectChanList";
        List<DetcChan> useDetcChanList = detcChanRepository.getUseDetcChanList();
        ValueOperations<String, String> stringStringValueOperations = redisTemplate.opsForValue();

        System.out.println(stringStringValueOperations.get(key).length());

        return stringStringValueOperations.get(key);
    }
    /*
     *  [2023.07.05] 사용 중인 감지채널의 저장이력 쿼리 Redis Server에 저장
     */
    public void setRedisDetcChanInstSqlList(){
//        String key = "DETC_CHAN_INST_SQL_";
        String key = "detcChanInstSql";
        List<DetcChanSqlInfo> useDetcChanSqlList = detcChanSqlInfoRepository.getUseDetcChanSqlList();

        ValueOperations<String, String> stringStringValueOperations = redisTemplate.opsForValue();

        byte[] decodedBytes =  null;
        String decodedString = "";

        //인코딩된 디코딩 후 업로드
        for(int i=0 ; i < useDetcChanSqlList.size() ; i++){
            if(useDetcChanSqlList.get(i).getSqlScrt().length() > 3) {
                decodedBytes = Base64.getDecoder().decode(useDetcChanSqlList.get(i).getSqlScrt().substring(3));
            }else{
                decodedBytes = Base64.getDecoder().decode(useDetcChanSqlList.get(i).getSqlScrt());
            }
            redisTemplate.delete(key+useDetcChanSqlList.get(i).getDetcChanCd());       //삭제 후 insert
            stringStringValueOperations.set(key+useDetcChanSqlList.get(i).getDetcChanCd(),
                                             new String(decodedBytes));
        }
        System.out.println("DETC_CHAN_LIST_SQL_INFO UPLOAD COMPLETE::::::::::::::::");

        getRedisDetcChanInstSqlList("9001");
    }

    /*
     *  [2023.07.05] Redis Server에서 감지채널 코드에 해당하는 쿼리 가져오기
     */
    public String getRedisDetcChanInstSqlList(String detcChanCd){
        String key = "detcChanInstSql"+detcChanCd;
        //List<DetcChanSqlInfo> useDetcChanSqlList = detcChanSqlInfoRepository.getUseDetcChanSqlList();

        ValueOperations<String, String> stringStringValueOperations = redisTemplate.opsForValue();
        System.out.println("GET INST QRY ::" + stringStringValueOperations.get(key).replaceAll("R_REBM_DETC_MSTR_1", "R_REBM_DETC_MSTR_"+this.getTableDtNum()));
        System.out.println("DETC_CHAN_LIST_SQL_INFO SELECT COMPLETE::::::::::::::::");
        stringStringValueOperations.get(key).replaceAll("R_REBM_DETC_MSTR_1", "R_REBM_DETC_MSTR_"+this.getTableDtNum());
        return stringStringValueOperations.get(key);
    }

    /*
     *  [2023.07.05] 감지채널 저장이력 쿼리 RDB 조회후 가져오기
     */
    public String getDetcChanInstSqlList(String detcChanCd) {
        System.out.println(detcChanSqlInfoRepository.findByOne(detcChanCd).replaceAll("R_REBM_DETC_MSTR_1", "R_REBM_DETC_MSTR_"+this.getTableDtNum()));
        return detcChanSqlInfoRepository.findByOne(detcChanCd).replaceAll("R_REBM_DETC_MSTR_1", "R_REBM_DETC_MSTR_"+this.getTableDtNum());
    }

    public void redistTest(){
        String key = "test";
        ListOperations<String, String> stringListOperations = redisTemplate.opsForList();
        stringListOperations.rightPush(key,"Hd");
        stringListOperations.rightPush(key,"ed");
        stringListOperations.rightPush(key,"lwww");
        stringListOperations.rightPush(key,"laaaa");
        stringListOperations.rightPush(key,"oaaa");

        for(int i = 0 ; i < stringListOperations.size(key);i++){
            System.out.println(stringListOperations.leftPop(key));
        }
//       System.out.println(stringListOperations);

    }
}
