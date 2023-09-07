package com.realtime.streamer.Queue;

import java.util.LinkedList;

public class ClanExQueue {
    static LinkedList<String> clanWorkQ = new LinkedList<>();
    static LinkedList<String> clanProdQ = new LinkedList<>();


    //Work Queue
    public LinkedList<String> getClanWorkQ() {
        return clanWorkQ;
    }

    public void setClanWorkQ(LinkedList<String> clanWorkQ) {
        this.clanWorkQ = clanWorkQ;

    }

    public void addWorkQueueItem(String item){
        this.clanWorkQ.add(item);
    }

    public String getWorkQueueItem(){
        String retItem = this.clanWorkQ.getFirst();
        this.clanWorkQ.remove();
        return retItem;
    }

    public void clearWorkQ(){
        this.clanWorkQ.clear();
    }


    //Producing Queue
    public LinkedList<String> getClanProdQ() {
        return clanProdQ;
    }

    public void setClanProdQ(LinkedList<String> clanProdQ) {
        this.clanProdQ = clanProdQ;

    }

    public void addProdQueueItem(String item){
        this.clanProdQ.add(item);
    }

    public String getProdQueueItem(){
        String retItem = this.clanProdQ.getFirst();
        this.clanProdQ.remove();
        return retItem;
    }

    public void clearProdQ(){
        this.clanProdQ.clear();
    }
}
