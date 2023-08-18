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

/* �ʿ� �޼ҵ� ����
 * [2023.07.05] �űԻ���
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class Utility {
    /*
     *  [2023.07.05] ���Ϻ� ���̺��� ���ϱ� ���� ��¥ ��ȸ
     *               ���� DayOfWeek Ŭ���� ���� 1,2,3,4,5,6,7(��,ȭ,��,��,��,��,��)
     *                               ���� ���� 2,3,4,5,6,7,1(��,ȭ,��,��,��,��,��) -> 0�� Simulation
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

    /**
     *
     * @param info : �����ɸ��� �۾�
     * @param ptagSqlId : sql ����
     */
    public void autoAlarmSave(String info, String ptagSqlId) {
        log.info("auto alarm test code:::::::::::::::::::::");
    }

    /**
     * kafka Producer ����
     * @return Producer Config return;
     */
    public Properties setKafkaProducerConfigs(String Address){
        System.out.println("utility call properties ::::::::::::::::qwe:" );
        log.info("log info utility call properties ::::::::::::::::retert:" );

        Properties configs = new Properties();
        configs.put("bootstrap.servers", Address); // kafka server host �� port //192.168.20.99:9092,192.168.20.100:9092,192.168.20.101:9092
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");   // serialize ����
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // serialize ����
        configs.put("acks", "all");                         // �ڽ��� ���� �޽����� ���� ī��ī�κ��� Ȯ���� ��ٸ��� �ʽ��ϴ�.
        configs.put("block.on.buffer.full", "true");        // ������ ���� ���ڵ带 ���۸� �� �� ����� �� �ִ� ��ü �޸��� ����Ʈ��

        System.out.println("utility call properties :::::::::::::::::" + configs);
        log.info("log info utility call properties :::::::::::::::::" + configs);
        return configs;
    }
    /**
     * kafka Consumer ����
     * @return Consumer Config return;
     */
    public Properties setKafkaConsumerConfigs(String Address, String GroupId){
        Properties configs = new Properties();
        configs.put("bootstrap.servers", Address); // kafka server host �� port    //192.168.20.99:9092,192.168.20.100:9092,192.168.20.101:9092
        configs.put("session.timeout.ms", "10000"); // session ����
        configs.put("group.id", GroupId); // �׷���̵� ����
        configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // key deserializer
        configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // value deserializer
        configs.put("auto.offset.reset", "latest"); // earliest(ó������ ����) | latest(������� ����)
        configs.put("enable.auto.commit", false); //AutoCommit ����

        System.out.println("utility call properties :::::::::::::::::" + configs);
        log.info("log info utility call properties :::::::::::::::::" + configs);


        return configs;
    }
    /**
     * REBM rule���μ��� h2 db ������ ����
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
//    ���̺��� �������� �� ���� ${������} ���
//        column = #{������} => column = '������'
//        FROM ${���̺��} => FROM ���̺��