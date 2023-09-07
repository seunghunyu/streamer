package com.realtime.streamer.Queue;

import java.util.LinkedList;

public class ChanExQueue {
    static LinkedList<String> assignWorkQ = new LinkedList<>();
    static LinkedList<String> assignProdQ = new LinkedList<>();


    //Work Queue
    public LinkedList<String> getAssignWorkQ() {
        return assignWorkQ;
    }

    public void setAssignWorkQ(LinkedList<String> assignWorkQ) {
        this.assignWorkQ = assignWorkQ;

    }

    public void addWorkQueueItem(String item){
        this.assignWorkQ.add(item);
    }

    public String getWorkQueueItem(){
        String retItem = this.assignWorkQ.getFirst();
        this.assignWorkQ.remove();
        return retItem;
    }

    public void clearWorkQ(){
        this.assignWorkQ.clear();
    }


    //Producing Queue
    public LinkedList<String> getAssignProdQ() {
        return assignProdQ;
    }

    public void setAssignProdQ(LinkedList<String> assignProdQ) {
        this.assignProdQ = assignProdQ;

    }

    public void addProdQueueItem(String item){
        this.assignProdQ.add(item);
    }

    public String getProdQueueItem(){
        String retItem = this.assignProdQ.getFirst();
        this.assignProdQ.remove();
        return retItem;
    }

    public void clearProdQ(){
        this.assignProdQ.clear();
    }


}
