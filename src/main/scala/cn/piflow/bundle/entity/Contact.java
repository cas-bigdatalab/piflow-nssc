package cn.piflow.bundle.entity;


import java.io.Serializable;

/**
  * @Title: Principle.java
  * @Project: csai-api
  * @Description: 历届责任人
  * @Author 路长发
  * @Date 2019/3/27 13:48
  * @Version V1.0
  */
public class Contact implements Serializable {


    private String contactType;
    private String number;


    public String getContactType() {
        return contactType;
    }

    public void setContactType(String contactType) {
        this.contactType = contactType;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }
}
