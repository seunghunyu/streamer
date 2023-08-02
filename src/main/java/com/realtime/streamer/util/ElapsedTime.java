package com.realtime.streamer.util;
public class ElapsedTime
{
    private long start  = 0;
    private long current = 0;

    public ElapsedTime()
    {
        start = System.currentTimeMillis();
    }

    public long startTimer()
    {
        start = System.currentTimeMillis();
        current = start;
        return start;
    }

    public long getStartTim()
    {
        return start;
    }

    public long getElapsed()
    {
        current = System.currentTimeMillis();
        return (current - start) ;
    }

    private String fillZero(int val, int n)
    {
        String input = Integer.toString(val);

        try
        {
            while (input.length() < n)
            { input = "0" + input; }
            return input;
        }
        catch( Exception e) { return ""; }
    }

    public String finishTime()
    {
        String rets;
        int dd, hh,mm,ss,ms;
        long delay, delayTime;

        delay     = System.currentTimeMillis() - start;
        ms        = (int)(delay % 1000);
        delayTime = delay / 1000;

        dd  =  (int) (delayTime / 86400); // 3600*24
        delayTime =  (int) (delayTime % 86400);
        hh  =  (int) (delayTime / 3600);
        mm  =  (int)((delayTime % 3600) / 60);
        ss  =  (int) (delayTime % 60);

        if(dd != 0)
            rets = dd + " days,  " + fillZero(hh, 2) + ":" + fillZero(mm, 2) + ":" + fillZero(ss, 2);
        else
            rets = fillZero(hh, 2) + ":" + fillZero(mm, 2) + ":" + fillZero(ss, 2);

        return rets.toString();
    }

    public String finishTimeMS()
    {
        String rets;
        int dd, hh,mm,ss,ms;
        long delay, delayTime;

        delay     = System.currentTimeMillis() - start;
        ms        = (int)(delay % 1000);
        delayTime = delay / 1000;

        dd  =  (int) (delayTime / 86400); // 3600*24
        delayTime =  (int) (delayTime % 86400);
        hh  =  (int) (delayTime / 3600);
        mm  =  (int)((delayTime % 3600) / 60);
        ss  =  (int) (delayTime % 60);

        if(dd != 0)
            rets = dd + " days,  " + fillZero(hh, 2) + ":" + fillZero(mm, 2) + ":" + fillZero(ss, 2);
        else
            rets = fillZero(hh, 2) + ":" + fillZero(mm, 2) + ":" + fillZero(ss, 2)+"."+fillZero(ms, 3);

        return rets.toString();
    }

    public String finishShortTimeMS()
    {
        String rets;
        int dd, hh,mm,ss,ms;
        long delay, delayTime;

        delay     = System.currentTimeMillis() - start;
        ms        = (int)(delay % 1000);
        delayTime = delay / 1000;

        dd  =  (int) (delayTime / 86400); // 3600*24
        delayTime =  (int) (delayTime % 86400);
        hh  =  (int) (delayTime / 3600);
        mm  =  (int)((delayTime % 3600) / 60);
        ss  =  (int) (delayTime % 60);

        if(dd != 0)
            rets = dd + " days,  " + fillZero(hh, 2) + ":" + fillZero(mm, 2) + ":" + fillZero(ss, 2);
        else
        if(hh == 0  && mm == 0)
            rets = fillZero(ss, 2)+"."+fillZero(ms, 3);
        else
        if(hh == 0  && mm != 0)
            rets = fillZero(mm, 2) + ":" + fillZero(ss, 2)+"."+fillZero(ms, 3);
        else
            rets = fillZero(hh, 2) + ":" + fillZero(mm, 2) + ":" + fillZero(ss, 2)+"."+fillZero(ms, 3);

        return rets.toString();
    }


}
