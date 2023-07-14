package com.realtime.streamer.repository;

import com.realtime.streamer.data.*;

import java.util.List;

public interface HistorySaveRepository {
    boolean saveDetc(DetcMstr detcMstr, String qry);
    boolean saveRuleS(RuleExS ruleExS, String qry);
    boolean saveRuleF(RuleExF ruleExF, String qry);
    boolean saveClan(ClanEx clanEx, String qry);
    boolean saveChan(ChanEx chanEx, String qry);

    boolean batchInsertDetc(List<DetcMstr> detcList, String qry);
    boolean batchInsertRuleS(List<RuleExS> ruleSList, String qry);
    boolean batchInsertRuleF(List<RuleExF> ruleFList, String qry);
    boolean batchInsertClan(List<ClanEx> clanList, String qry);
    boolean batchInsertChan(List<ChanEx> chanList, String qry);
}
