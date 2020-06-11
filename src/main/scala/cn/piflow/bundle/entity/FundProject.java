package cn.piflow.bundle.entity;


import java.io.Serializable;

public class FundProject implements Serializable {


    private String projectId;
    private String projectName;


    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }
}
