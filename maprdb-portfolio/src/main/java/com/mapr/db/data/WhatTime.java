package com.mapr.db.data;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public final class WhatTime {

    private SimpleDateFormat _sdf               = new SimpleDateFormat("yyyy:MM:dd-HH:mm:ss,S");
    private static WhatTime  _whatTimeSingleton = null;

    protected WhatTime() {
        // Exists only to defeat instantiation.
    }

    public synchronized String whatTimeIsIt() {
        return _sdf.format(Calendar.getInstance().getTime()) + " ";
    }

    public synchronized static WhatTime getInstance() {
        if (_whatTimeSingleton == null)
            _whatTimeSingleton = new WhatTime();

        return _whatTimeSingleton;
    }

    /**
     * Turn a number of milliseconds into a formatted string that contains hours, minutes and seconds.
     * 
     * @param milliseconds
     * @return String
     */
    public synchronized static String convertMsToHMS(long milliseconds) {

        float ms = (float) milliseconds;

        long hours = (long) (ms / (1000.0f * 3600.0f));
        if (hours < 0)
            hours = 0;

        ms -= (float) (hours * 1000 * 3600);

        int minutes = (int) (ms / (1000.0f * 60.0f));
        if (minutes < 0)
            minutes = 0;

        ms -= (float) (minutes * 1000 * 60);

        int seconds = (int) (ms / 1000.0f);
        if (seconds < 0)
            seconds = 0;

        return new String(hours + "h:" + minutes + "m:" + seconds + "s");
    }

}