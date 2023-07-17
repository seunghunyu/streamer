package com.realtime.streamer.util;

import com.realtime.streamer.data.DetcChan;
import com.realtime.streamer.data.DetcChanSql;
import com.realtime.streamer.data.DetcChanSqlInfo;
import com.realtime.streamer.repository.JdbcTemplateDetcChanRepository;
import com.realtime.streamer.repository.JdbcTemplateDetcChanSqlRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/* 필요 메소드 정의
 * [2023.07.05] 신규생성
 */
@Component
@RequiredArgsConstructor
public class Utility {
    /*
     *  [2023.07.05] 요일별 테이블을 구하기 위한 날짜 조회
     *               기존 DayOfWeek 클래스 기준 1,2,3,4,5,6,7(월,화,수,목,금,토,일)
     *                               현재 기준 2,3,4,5,6,7,1(월,화,수,목,금,토,일) -> 0은 Simulation
     */
    @Autowired
    JdbcTemplateDetcChanRepository detcChanRepository;

    @Autowired
    JdbcTemplateDetcChanSqlRepository detcChanSqlRepository;

    @Autowired
    StringRedisTemplate redisTemplate;

    @Autowired
    private final JdbcTemplate jdbcTemplate;

    public String getTableDtNum() {
        LocalDateTime date = LocalDate.now().atStartOfDay();
        //System.out.println("dayofWeekNumber" + Integer.toString(dayOfWeekNumber);
//        return Integer.toString(date.getDayOfWeek().getValue()-1);
        return Integer.toString(date.getDayOfWeek().getValue());
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

        System.out.println("REDIS DETC_CHAN_LIST UPLOAD COMPLETE::::::::::::::::");
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
//        String key = "DETC_CHAN_INST_SQL_";       key 세팅시 특수문자 사용하면 에러 발생
        String key = "detcChanInstSql";
        ////List<DetcChanSql> useDetcChanSqlList = detcChanSqlRepository.getUseDetcChanSqlList(); //조회 쿼리
//        List<DetcChanSql> useDetcChanSqlList = detcChanSqlRepository.findByOne("9001");

        //사용중인 감지채널
        List<DetcChan> userDetcChan = detcChanRepository.getUseDetcChanList();

        ValueOperations<String, String> stringStringValueOperations = redisTemplate.opsForValue();

        byte[] decodedBytes =  null;
        String decodedString = "";

        //인코딩된 디코딩 후 업로드
        for(int i=0 ; i < userDetcChan.size() ; i++){

            String scrt = detcChanSqlRepository.findByOne(userDetcChan.get(i).getDetcChanCd());

            System.out.println("SCRT:::::::::@@@@@@@@@@@@@@@@@"+scrt);

//            if(scrt.length() > 3) {
//                decodedBytes = Base64.getDecoder().decode(scrt.substring(3));
//            }else{
//                decodedBytes = Base64.getDecoder().decode(scrt);
//            }


            redisTemplate.delete(key+userDetcChan.get(i).getDetcChanCd());       //삭제 후 insert
            stringStringValueOperations.set(key+userDetcChan.get(i).getDetcChanCd(), scrt);
//            stringStringValueOperations.set(key+userDetcChan.get(i).getDetcChanCd(), new String(decodedBytes));
        }
        System.out.println("REDIS DETC CHAN INST QRY UPLOAD COMPLETE::::::::::::::::");

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
        String detcQry = stringStringValueOperations.get(key).replaceAll("R_REBM_DETC_MSTR_1", "R_REBM_DETC_MSTR_"+this.getTableDtNum());
        return detcQry;
    }

    /*
     *  [2023.07.05] 감지채널 저장이력 쿼리 RDB 조회후 가져오기
     */
    public String getDetcChanInstSqlList(String detcChanCd) {
        System.out.println(detcChanSqlRepository.findByOne(detcChanCd).replaceAll("R_REBM_DETC_MSTR_1", "R_REBM_DETC_MSTR_"+this.getTableDtNum()));
        return detcChanSqlRepository.findByOne(detcChanCd).replaceAll("R_REBM_DETC_MSTR_1", "R_REBM_DETC_MSTR_"+this.getTableDtNum());
    }

    /*
     *  [2023.07.17] 감지채널 사용 부가정보 아이템 Redis 서버 세팅
     */
    public void setRedisDetcChanAddInfoItem(){
        String key = "detcChanAddInfoItem";
        List<DetcChanSql> useDetcChanSqlList = detcChanSqlRepository.getUseDetcChanSqlList();



        ValueOperations<String, String> stringStringValueOperations = redisTemplate.opsForValue();

        for(int i = 0 ; i < useDetcChanSqlList.size() ; i++){
            String addInfoList = "";
            if(getDetcChanAddInfoList(useDetcChanSqlList.get(i).getDetcChanCd()) == null) addInfoList = "";
            else addInfoList = getDetcChanAddInfoList(useDetcChanSqlList.get(i).getDetcChanCd());
            redisTemplate.delete(key+useDetcChanSqlList.get(i).getDetcChanCd());       //삭제 후 insert
            stringStringValueOperations.set(key+useDetcChanSqlList.get(i).getDetcChanCd(), addInfoList);

        }
        System.out.println("DETC_CHAN_LIST_SQL_ADD_INFO_ITEM UPLOAD COMPLETE::::::::::::::::");
    }
    /*
     *  [2023.07.17] 감지채널 사용 부가정보 아이템 Redis 서버에서 가져오기
     */
    public String getRedisDetcChanAddInfoItem(String detcChancd){
        String key = "detcChanAddInfoItem"+detcChancd;

        ValueOperations<String, String> stringStringValueOperations = redisTemplate.opsForValue();
        System.out.println("DETC_CHAN_LIST_SQL_ADD_INFO_ITEM SELECT COMPLETE::::::::::::::::");
        String item = stringStringValueOperations.get(key);

        return item;
    }


    /*
     *  [2023.07.17] RDB 조회 이용 감지채널 사용 부가정보 아이템 데이터 가져오기
     */
    public String getDetcChanAddInfoList(String detcChanCd){
        String qry = " SELECT DETC_CHAN_CD, SQL_KIND, DB_POOL, SEL_ITEM, SEL_TYPE FROM R_REBM_DETC_CHAN_SQL_INFO WHERE DETC_CHAN_CD = ? AND SQL_KIND = ? ";

       List<DetcChanSqlInfo> results =  jdbcTemplate.query(qry, new RowMapper<DetcChanSqlInfo>() {
            @Override
            public DetcChanSqlInfo mapRow(ResultSet rs, int rowNum) throws SQLException {
                DetcChanSqlInfo sqlInfo = new DetcChanSqlInfo();
                sqlInfo.setDetcChanCd(rs.getString("DETC_CHAN_CD"));
                sqlInfo.setSqlKind(rs.getString("SQL_KIND"));
                sqlInfo.setDbPool(rs.getString("DB_POOL"));
                sqlInfo.setSelItem(rs.getString("SEL_ITEM"));
                sqlInfo.setSelType(rs.getString("SEL_TYPE"));

                return sqlInfo;
            }
        }, detcChanCd, "5");


        return results.isEmpty() ? null : results.get(0).getSelItem();
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
