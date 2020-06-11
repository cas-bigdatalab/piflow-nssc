package cn.piflow.bundle.entity;


import java.io.Serializable;


public class Reference implements Serializable {


    private String referencePaperId;
    private String referencePaperName;

    public String getReferencePaperId() {
        return referencePaperId;
    }

    public void setReferencePaperId(String referencePaperId) {
        this.referencePaperId = referencePaperId;
    }

    public String getReferencePaperName() {
        return referencePaperName;
    }

    public void setReferencePaperName(String referencePaperName) {
        this.referencePaperName = referencePaperName;
    }
}
