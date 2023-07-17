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

/* �ʿ� �޼ҵ� ����
 * [2023.07.05] �űԻ���
 */
@Component
@RequiredArgsConstructor
public class Utility {
    /*
     *  [2023.07.05] ���Ϻ� ���̺��� ���ϱ� ���� ��¥ ��ȸ
     *               ���� DayOfWeek Ŭ���� ���� 1,2,3,4,5,6,7(��,ȭ,��,��,��,��,��)
     *                               ���� ���� 2,3,4,5,6,7,1(��,ȭ,��,��,��,��,��) -> 0�� Simulation
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
     *  [2023.07.05] ��� ���� ����ä�� ����Ʈ ���ε�
     *               Redis Server�� ���ε�
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
        //�ش� Ű�� value�� ����� ���� �� ���ε�
        redisTemplate.delete(key);

        for(int i=0 ; i < useDetcChanList.size() ; i++){
            //KEY ���� REBM_DETC_LIST -> '_' �Է½� ���� �߻�
            stringListOperations.rightPush(key, useDetcChanList.get(i).getDetcChanCd());
        }

        System.out.println("REDIS DETC_CHAN_LIST UPLOAD COMPLETE::::::::::::::::");
//        List<String> ResultRange = stringListOperations.range(key, 0, stringListOperations.size(key));
        System.out.println(stringListOperations.range(key, 0, stringListOperations.size(key)));

    }

    /*
     *  [2023.07.05] ��� ���� ����ä�� ����Ʈ Redis Server���� ��������
     */
    public String getRedisDetcChanList(){
        String key = "dectChanList";
        List<DetcChan> useDetcChanList = detcChanRepository.getUseDetcChanList();
        ValueOperations<String, String> stringStringValueOperations = redisTemplate.opsForValue();

        System.out.println(stringStringValueOperations.get(key).length());

        return stringStringValueOperations.get(key);
    }
    /*
     *  [2023.07.05] ��� ���� ����ä���� �����̷� ���� Redis Server�� ����
     */
    public void setRedisDetcChanInstSqlList(){
//        String key = "DETC_CHAN_INST_SQL_";       key ���ý� Ư������ ����ϸ� ���� �߻�
        String key = "detcChanInstSql";
        ////List<DetcChanSql> useDetcChanSqlList = detcChanSqlRepository.getUseDetcChanSqlList(); //��ȸ ����
//        List<DetcChanSql> useDetcChanSqlList = detcChanSqlRepository.findByOne("9001");

        //������� ����ä��
        List<DetcChan> userDetcChan = detcChanRepository.getUseDetcChanList();

        ValueOperations<String, String> stringStringValueOperations = redisTemplate.opsForValue();

        byte[] decodedBytes =  null;
        String decodedString = "";

        //���ڵ��� ���ڵ� �� ���ε�
        for(int i=0 ; i < userDetcChan.size() ; i++){

            String scrt = detcChanSqlRepository.findByOne(userDetcChan.get(i).getDetcChanCd());

            System.out.println("SCRT:::::::::@@@@@@@@@@@@@@@@@"+scrt);

//            if(scrt.length() > 3) {
//                decodedBytes = Base64.getDecoder().decode(scrt.substring(3));
//            }else{
//                decodedBytes = Base64.getDecoder().decode(scrt);
//            }


            redisTemplate.delete(key+userDetcChan.get(i).getDetcChanCd());       //���� �� insert
            stringStringValueOperations.set(key+userDetcChan.get(i).getDetcChanCd(), scrt);
//            stringStringValueOperations.set(key+userDetcChan.get(i).getDetcChanCd(), new String(decodedBytes));
        }
        System.out.println("REDIS DETC CHAN INST QRY UPLOAD COMPLETE::::::::::::::::");

    }

    /*
     *  [2023.07.05] Redis Server���� ����ä�� �ڵ忡 �ش��ϴ� ���� ��������
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
     *  [2023.07.05] ����ä�� �����̷� ���� RDB ��ȸ�� ��������
     */
    public String getDetcChanInstSqlList(String detcChanCd) {
        System.out.println(detcChanSqlRepository.findByOne(detcChanCd).replaceAll("R_REBM_DETC_MSTR_1", "R_REBM_DETC_MSTR_"+this.getTableDtNum()));
        return detcChanSqlRepository.findByOne(detcChanCd).replaceAll("R_REBM_DETC_MSTR_1", "R_REBM_DETC_MSTR_"+this.getTableDtNum());
    }

    /*
     *  [2023.07.17] ����ä�� ��� �ΰ����� ������ Redis ���� ����
     */
    public void setRedisDetcChanAddInfoItem(){
        String key = "detcChanAddInfoItem";
        List<DetcChanSql> useDetcChanSqlList = detcChanSqlRepository.getUseDetcChanSqlList();



        ValueOperations<String, String> stringStringValueOperations = redisTemplate.opsForValue();

        for(int i = 0 ; i < useDetcChanSqlList.size() ; i++){
            String addInfoList = "";
            if(getDetcChanAddInfoList(useDetcChanSqlList.get(i).getDetcChanCd()) == null) addInfoList = "";
            else addInfoList = getDetcChanAddInfoList(useDetcChanSqlList.get(i).getDetcChanCd());
            redisTemplate.delete(key+useDetcChanSqlList.get(i).getDetcChanCd());       //���� �� insert
            stringStringValueOperations.set(key+useDetcChanSqlList.get(i).getDetcChanCd(), addInfoList);

        }
        System.out.println("DETC_CHAN_LIST_SQL_ADD_INFO_ITEM UPLOAD COMPLETE::::::::::::::::");
    }
    /*
     *  [2023.07.17] ����ä�� ��� �ΰ����� ������ Redis �������� ��������
     */
    public String getRedisDetcChanAddInfoItem(String detcChancd){
        String key = "detcChanAddInfoItem"+detcChancd;

        ValueOperations<String, String> stringStringValueOperations = redisTemplate.opsForValue();
        System.out.println("DETC_CHAN_LIST_SQL_ADD_INFO_ITEM SELECT COMPLETE::::::::::::::::");
        String item = stringStringValueOperations.get(key);

        return item;
    }


    /*
     *  [2023.07.17] RDB ��ȸ �̿� ����ä�� ��� �ΰ����� ������ ������ ��������
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
