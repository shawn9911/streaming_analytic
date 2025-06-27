package com.streaming.analytic.dto;

public class HourlyCount {
    private int count;
    private String hour;

    public HourlyCount() {}

    public HourlyCount(int count, String hour) {
        this.count = count;
        this.hour = hour;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }
}
