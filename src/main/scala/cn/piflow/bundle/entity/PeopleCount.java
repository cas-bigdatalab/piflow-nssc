package cn.piflow.bundle.entity;


import java.io.Serializable;


public class PeopleCount implements Serializable {


    private String year;
    private String count;


    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getCount() {
        return count;
    }

    public void setCount(String count) {
        this.count = count;
    }
}
