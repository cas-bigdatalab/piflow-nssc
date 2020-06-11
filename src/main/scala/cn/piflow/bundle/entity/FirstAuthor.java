package cn.piflow.bundle.entity;


import java.io.Serializable;


public class FirstAuthor implements Serializable {


    private String personName;
    private String personId;


    public String getPersonName() {
        return personName;
    }

    public void setPersonName(String personName) {
        this.personName = personName;
    }

    public String getPersonId() {
        return personId;
    }

    public void setPersonId(String personId) {
        this.personId = personId;
    }
}
