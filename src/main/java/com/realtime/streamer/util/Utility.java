package com.realtime.streamer.util;

import com.realtime.streamer.data.DetcChan;
import com.realtime.streamer.data.DetcChanSqlInfo;
import com.realtime.streamer.repository.JdbcTemplateCampRepository;
import com.realtime.streamer.repository.JdbcTemplateDetcChanRepository;
import com.realtime.streamer.repository.JdbcTemplateDetcChanSqlInfoRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/* �ʿ� �޼ҵ� ����
 * [2023.07.05] �űԻ���
 */
public class Utility {
    /*
     *  [2023.07.05] ���Ϻ� ���̺��� ���ϱ� ���� ��¥ ��ȸ
     *               ���� DayOfWeek Ŭ���� ���� 1,2,3,4,5,6,7(��,ȭ,��,��,��,��)
     *                               ���� ���� 2,3,4,5,6,7,1(��,ȭ,��,��,��,��) -> 0�� Simulation
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
     *  [2023.07.05] ��� ���� ����ä�� ����Ʈ ���ε�
     *               Redis Server�� ���ε�
     */
    public void setRedisDetcChanList(){
        String key = "DETC_CHAN_LIST";
        List<DetcChan> useDetcChanList = detcChanRepository.getUseDetcChanList();
        ValueOperations<String, String> stringStringValueOperations = redisTemplate.opsForValue();
        for(int i=0 ; i < useDetcChanList.size() ; i++){
            stringStringValueOperations.set(key, useDetcChanList.get(i).getDetcChanCd());
        }

        System.out.println("DETC_CHAN_LIST UPLOAD COMPLETE::::::::::::::::");
    }

    /*
     *  [2023.07.05] ��� ���� ����ä�� ����Ʈ Redis Server���� ��������
     */
    public String getRedisDetcChanList(){
        String key = "DETC_CHAN_LIST";
        List<DetcChan> useDetcChanList = detcChanRepository.getUseDetcChanList();
        ValueOperations<String, String> stringStringValueOperations = redisTemplate.opsForValue();
        return stringStringValueOperations.get(key);
    }

    public void setRedisDetcChanInstSqlList(){
        String key = "DETC_CHAN_INST_SQL_";
        List<DetcChanSqlInfo> useDetcChanSqlList = detcChanSqlInfoRepository.getUseDetcChanSqlList();

        ValueOperations<String, String> stringStringValueOperations = redisTemplate.opsForValue();

        byte[] decodedBytes =  null;
        String decodedString = "";

        //���ڵ��� ���ڵ� �� ���ε�
        for(int i=0 ; i < useDetcChanSqlList.size() ; i++){
            if(useDetcChanSqlList.get(i).getSqlScrt().length() > 3) {
                decodedBytes = Base64.getDecoder().decode(useDetcChanSqlList.get(i).getSqlScrt().substring(3));
            }else{
                decodedBytes = Base64.getDecoder().decode(useDetcChanSqlList.get(i).getSqlScrt());
            }

            stringStringValueOperations.set(key+useDetcChanSqlList.get(i).getDetcChanCd(),
                                             new String(decodedBytes));
        }
        System.out.println("DETC_CHAN_LIST_SQL_INFO UPLOAD COMPLETE::::::::::::::::");
    }
    public void getRedisDetcChanInstSqlList(){
        String key = "DETC_CHAN_INST_SQL_";
        List<DetcChanSqlInfo> useDetcChanSqlList = detcChanSqlInfoRepository.getUseDetcChanSqlList();

        ValueOperations<String, String> stringStringValueOperations = redisTemplate.opsForValue();

        List<String> instQryList = new ArrayList<>();

        byte[] decodedBytes =  null;
        String decodedString = "";

        //���ڵ��� ���ڵ� �� ���ε�
        for(int i=0 ; i < useDetcChanSqlList.size() ; i++){
            if(useDetcChanSqlList.get(i).getSqlScrt().length() > 3) {
                decodedBytes = Base64.getDecoder().decode(useDetcChanSqlList.get(i).getSqlScrt().substring(3));
            }else{
                decodedBytes = Base64.getDecoder().decode(useDetcChanSqlList.get(i).getSqlScrt());
            }
            System.out.println("GET INST QRY ::" + stringStringValueOperations.get(key+useDetcChanSqlList.get(i).getDetcChanCd()));
            instQryList.add(stringStringValueOperations.get(key+useDetcChanSqlList.get(i).getDetcChanCd()));
        }

        System.out.println("DETC_CHAN_LIST_SQL_INFO SELECT COMPLETE::::::::::::::::");
    }
}
