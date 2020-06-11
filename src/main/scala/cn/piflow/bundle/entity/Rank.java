package cn.piflow.bundle.entity;


import java.io.Serializable;

public class Rank implements Serializable {


    private String rankGrade;
    //private String rankNo;

    private String rankName;


    public String getRankGrade() {
        return rankGrade;
    }

    public void setRankGrade(String rankGrade) {
        this.rankGrade = rankGrade;
    }

   /* public String getRankNo() {
        return rankNo;
    }

    public void setRankNo(String rankNo) {
        this.rankNo = rankNo;
    }*/

    public String getRankName() {
        return rankName;
    }

    public void setRankName(String rankName) {
        this.rankName = rankName;
    }
}
