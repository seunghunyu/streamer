package com.realtime.streamer.util;

import com.realtime.streamer.data.DetcChan;
import com.realtime.streamer.data.DetcChanSql;
import com.realtime.streamer.data.DetcChanSqlInfo;
import com.realtime.streamer.repository.rebm.JdbcTemplateDataLoadRepository;
import com.realtime.streamer.repository.rebm.JdbcTemplateDetcChanRepository;
import com.realtime.streamer.repository.rebm.JdbcTemplateDetcChanSqlRepository;
import com.realtime.streamer.service.DataLoadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/* 필요 메소드 정의
 * [2023.07.05] 신규생성
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class Utility {
    /*
     *  [2023.07.05] 요일별 테이블을 구하기 위한 날짜 조회
     *               기존 DayOfWeek 클래스 기준 1,2,3,4,5,6,7(월,화,수,목,금,토,일)
     *                               현재 기준 2,3,4,5,6,7,1(월,화,수,목,금,토,일) -> 0은 Simulation
     */
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";

    @Autowired
    //@Qualifier("rebmJdbcTemplate")
    JdbcTemplateDetcChanRepository detcChanRepository;

    @Autowired
    //@Qualifier("rebmJdbcTemplate")
    JdbcTemplateDetcChanSqlRepository detcChanSqlRepository;

    @Autowired
    DataLoadService dataLoadService;



    @Autowired
    StringRedisTemplate redisTemplate;

    @Autowired
    //@Qualifier("rebmJdbcTemplate")
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

    /**
     *
     * @param info : 오래걸리는 작업
     * @param ptagSqlId : sql 정보
     */
    public void autoAlarmSave(String info, String ptagSqlId) {
        log.info("auto alarm test code:::::::::::::::::::::");
    }

    /**
     * kafka Producer 세팅
     * @return Producer Config return;
     */
    public Properties setKafkaProducerConfigs(String Address){
        System.out.println("utility call properties ::::::::::::::::qwe:" );
        log.info("log info utility call properties ::::::::::::::::retert:" );

        Properties configs = new Properties();
        configs.put("bootstrap.servers", Address); // kafka server host 및 port //192.168.20.99:9092,192.168.20.100:9092,192.168.20.101:9092
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");   // serialize 설정
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // serialize 설정
        configs.put("acks", "all");                         // 자신이 보낸 메시지에 대해 카프카로부터 확인을 기다리지 않습니다.
        configs.put("block.on.buffer.full", "true");        // 서버로 보낼 레코드를 버퍼링 할 때 사용할 수 있는 전체 메모리의 바이트수

        System.out.println("utility call properties :::::::::::::::::" + configs);
        log.info("log info utility call properties :::::::::::::::::" + configs);
        return configs;
    }
    /**
     * kafka Consumer 세팅
     * @return Consumer Config return;
     */
    public Properties setKafkaConsumerConfigs(String Address, String GroupId){
        Properties configs = new Properties();
        configs.put("bootstrap.servers", Address); // kafka server host 및 port    //192.168.20.99:9092,192.168.20.100:9092,192.168.20.101:9092
        configs.put("session.timeout.ms", "10000"); // session 설정
        configs.put("group.id", GroupId); // 그룹아이디 설정
        configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // key deserializer
        configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // value deserializer
        configs.put("auto.offset.reset", "latest"); // earliest(처음부터 읽음) | latest(현재부터 읽음)
        configs.put("enable.auto.commit", false); //AutoCommit 여부

        System.out.println("utility call properties :::::::::::::::::" + configs);
        log.info("log info utility call properties :::::::::::::::::" + configs);


        return configs;
    }
    /**
     * REBM rule프로세스 h2 db 데이터 적재
     * @return
     */
    public void ruleUseDataLoader(){

        DocumentBuilderFactory factory = null;
        DocumentBuilder builder = null;
        JSONArray tblJArr = new JSONArray();
        String errorPart = "";
        try{
            factory = DocumentBuilderFactory.newInstance();
            builder = factory.newDocumentBuilder();

            Document document = builder.parse("xml/REBMJB.xml");

            NodeList locList = document.getElementsByTagName("LocalTables");
            for (int temp = 0; temp < locList.getLength(); temp++) {
                Node nNode = locList.item(temp);
                System.out.println("\nCurrent Element :" + nNode.getNodeName());

                NodeList locCList = nNode.getChildNodes();
                for(int i = 0 ; i < locCList.getLength() ; i++){
                    Node lCNode = locCList.item(i);

                    if(lCNode.getNodeName().equals("Table")){
                        //System.out.println(Integer.toString(i)+"::Table");
                        NodeList locCCNList = lCNode.getChildNodes();
                        //System.out.println(locCCNList.getLength());
                        JSONObject tblObj = new JSONObject();

                        tblObj.put("name",lCNode.getAttributes().getNamedItem("name").getNodeValue());
                        tblObj.put("desc",lCNode.getAttributes().getNamedItem("desc").getNodeValue());
                        tblObj.put("exceptionignore",lCNode.getAttributes().getNamedItem("exceptionignore").getNodeValue());
                        tblObj.put("selectdbpool",lCNode.getAttributes().getNamedItem("selectdbpool").getNodeValue());

                        JSONArray jArr = new JSONArray();
                        for(int j = 0 ; j < locCCNList.getLength() ; j++){
                            if(locCCNList.item(j).getNodeType() == Node.ELEMENT_NODE) {
                                JSONObject jobj = new JSONObject();
                                //System.out.println("   " + locCCNList.item(j).getNodeName());
                                if (locCCNList.item(j).getNodeName().equals("Insert")) {
                                    //System.out.println("Insert :: " + locCCNList.item(j).getTextContent());
                                    jobj.put("insert",locCCNList.item(j).getTextContent());
                                } else if (locCCNList.item(j).getNodeName().equals("Create")) {
                                    //System.out.println("Create :: " + locCCNList.item(j).getTextContent());
                                    jobj.put("create",locCCNList.item(j).getTextContent());
                                } else if (locCCNList.item(j).getNodeName().equals("Select")) {
                                    //System.out.println("Select :: " + locCCNList.item(j).getTextContent());
                                    jobj.put("select",locCCNList.item(j).getTextContent());
                                } else if (locCCNList.item(j).getNodeName().equals("Index")) {
                                    //System.out.println("Index :: " + locCCNList.item(j).getTextContent());
                                    jobj.put("index", locCCNList.item(j).getTextContent());
                                }
                                jArr.add(jobj);
                            }
                        }
                        tblObj.put("query", jArr);

                        tblJArr.add(tblObj);
                    }
                }

            }

            for(int i = 0 ; i < tblJArr.size() ; i++){
                JSONObject jsonObject  = (JSONObject) tblJArr.get(i);
                //System.out.println(Integer.toString(i)+"###################################"+jsonObject.toString());
                String tableName       = jsonObject.get("name")              != null ? jsonObject.get("name").toString() : "";
                String exceptionIgnore = jsonObject.get("exceptionignore")   != null ? jsonObject.get("exceptionignore").toString() : "";
                String selectdbpool    = jsonObject.get("selectdbpool")      != null ? jsonObject.get("selectdbpool").toString() : "";
                JSONArray jsonArray    = (JSONArray) jsonObject.get("query");
                System.out.println(Integer.toString(i)+"jsonArraySize()" + jsonArray.size());
                String indexQry  = "";
                String createQry = "";
                String selectQry = "";
                String insertQry = "";
                for(int j = 0 ; j < jsonArray.size() ; j++) {
                    JSONObject jobj = (JSONObject) jsonArray.get(j);
                    if(jobj.get("create") != null){
                        createQry = jobj.get("create").toString();
                    }else if(jobj.get("index") != null){
                        indexQry  = jobj.get("index").toString();
                    }else if(jobj.get("select") != null){
                        selectQry = jobj.get("select").toString();
                    }else if(jobj.get("insert") != null) {
                        insertQry = jobj.get("insert").toString();
                    }
                }
                try{
                    if(!createQry.equals("")) {
                        System.out.println(tableName+"create###################################"+createQry);
                        errorPart = "create";
                        dataLoadService.createTable(createQry);
                        if(!indexQry.equals("")){
                            System.out.println(tableName+"index###################################"+indexQry);
                            errorPart = "index";
                            dataLoadService.createIndex(indexQry);
                        }
                    }
                    if(!selectQry.equals("")){
                        errorPart = "select";
                        System.out.println(tableName+"select###################################"+selectQry);
                        List<?> objects = dataLoadService.selectData(selectQry, selectdbpool);

                        for(int k = 0 ; k < objects.size() ; k++){
                            log.info(objects.get(k).toString());
                            Map<String, Object> map = (Map<String, Object>) objects.get(k);

                            if(!insertQry.equals("")){
                                errorPart = "insert";
                                System.out.println(tableName+"insert###################################"+insertQry);
                                dataLoadService.insertData(insertQry, map);
                            }
                        }
                    }

                }catch(Exception ex){
                    ex.printStackTrace();
                    if(exceptionIgnore.equals("F")) System.out.println(tableName + " error occuring in " + errorPart);
                }

            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
//    테이블을 동적으로 할 때는 ${변수명} 사용
//        column = #{변수명} => column = '변수명'
//        FROM ${테이블명} => FROM 테이블명