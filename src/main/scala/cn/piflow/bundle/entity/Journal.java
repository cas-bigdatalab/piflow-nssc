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
public class Journal implements Serializable {


    private String journalName;
    private String journalId;


    public String getJournalName() {
        return journalName;
    }

    public void setJournalName(String journalName) {
        this.journalName = journalName;
    }

    public String getJournalId() {
        return journalId;
    }

    public void setJournalId(String journalId) {
        this.journalId = journalId;
    }
}
