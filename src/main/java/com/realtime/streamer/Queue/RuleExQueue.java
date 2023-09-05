package com.realtime.streamer.Queue;

import java.util.LinkedList;

public class RuleExQueue {
    static LinkedList<String> ruleWorkQ = new java.util.LinkedList<>();
    static LinkedList<String> ruleProdQ = new LinkedList<>();


    //Work Queue
    public LinkedList<String> getRuleWorkQ() {
        return ruleWorkQ;
    }

    public void setRuleWorkQ(LinkedList<String> assignWorkQ) {
        this.ruleWorkQ = assignWorkQ;

    }

    public void addWorkQueueItem(String item){
        this.ruleWorkQ.add(item);
    }

    public String getWorkQueueItem(){
        String retItem = this.ruleWorkQ.getFirst();
        this.ruleWorkQ.remove();
        return retItem;
    }

    public void clearWorkQ(){
        this.ruleWorkQ.clear();
    }


    //Producing Queue
    public LinkedList<String> getRuleProdQ() {
        return ruleProdQ;
    }

    public void setRuleProdQ(LinkedList<String> assignProdQ) {
        this.ruleProdQ = assignProdQ;

    }

    public void addProdQueueItem(String item){
        this.ruleProdQ.add(item);
    }

    public String getProdQueueItem(){
        String retItem = this.ruleProdQ.getFirst();
        this.ruleProdQ.remove();
        return retItem;
    }

    public void clearProdQ(){
        this.ruleProdQ.clear();
    }
}
