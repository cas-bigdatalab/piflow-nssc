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
public class Conference implements Serializable {


    private String conferenceName;
    private String conferenceId;


    public String getConferenceName() {
        return conferenceName;
    }

    public void setConferenceName(String conferenceName) {
        this.conferenceName = conferenceName;
    }

    public String getConferenceId() {
        return conferenceId;
    }

    public void setConferenceId(String conferenceId) {
        this.conferenceId = conferenceId;
    }
}
