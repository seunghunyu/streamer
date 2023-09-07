package com.realtime.streamer.Queue;

import java.util.LinkedList;

public class ChanExQueue {
    static LinkedList<String> chanWorkQ = new LinkedList<>();
    static LinkedList<String> chanProdQ = new LinkedList<>();


    //Work Queue
    public LinkedList<String> getAssignWorkQ() {
        return chanWorkQ;
    }

    public void setAssignWorkQ(LinkedList<String> assignWorkQ) {
        this.chanWorkQ = assignWorkQ;

    }

    public void addWorkQueueItem(String item){
        this.chanWorkQ.add(item);
    }

    public String getWorkQueueItem(){
        String retItem = this.chanWorkQ.getFirst();
        this.chanWorkQ.remove();
        return retItem;
    }

    public void clearWorkQ(){
        this.chanWorkQ.clear();
    }


    //Producing Queue
    public LinkedList<String> getAssignProdQ() {
        return chanProdQ;
    }

    public void setAssignProdQ(LinkedList<String> assignProdQ) {
        this.chanProdQ = assignProdQ;

    }

    public void addProdQueueItem(String item){
        this.chanProdQ.add(item);
    }

    public String getProdQueueItem(){
        String retItem = this.chanProdQ.getFirst();
        this.chanProdQ.remove();
        return retItem;
    }

    public void clearProdQ(){
        this.chanProdQ.clear();
    }


}
