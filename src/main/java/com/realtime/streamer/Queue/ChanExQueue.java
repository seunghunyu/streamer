package com.realtime.streamer.Queue;

import java.util.LinkedList;

public class ChanExQueue {
    static LinkedList<String> chanWorkQ = new LinkedList<>();
    static LinkedList<String> chanProdQ = new LinkedList<>();


    //Work Queue
    public LinkedList<String> getChanWorkQ() {
        return chanWorkQ;
    }

    public void setChanWorkQ(LinkedList<String> chanWorkQ) {
        this.chanWorkQ = chanWorkQ;

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
    public LinkedList<String> getChanProdQ() {
        return chanProdQ;
    }

    public void setChanProdQ(LinkedList<String> chanProdQ) {
        this.chanProdQ = chanProdQ;

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
