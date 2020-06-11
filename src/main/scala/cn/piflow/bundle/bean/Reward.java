package cn.piflow.bundle.bean;

import java.io.Serializable;

public class Reward implements Serializable {
    String rewardId;
    String name;
    String etablishment;
    String organizer;
    String abbreviation;
    String awardType;
    String awardClass;
    String awardGrade;
    String project;
    String projectPerson ;
    String projectOrg;
    String paper;
    String paperAuthor;
    String person;
    String awardOrg;
    String recommendOrg;
    String awardDate;
    String place;
    String source;
    String lastUpdate;
    String commonId;
    int year;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getCommonId() {
        return commonId;
    }

    public void setCommonId(String commonId) {
        this.commonId = commonId;
    }

    public String getAwardDate() {
        return awardDate;
    }

    public void setAwardDate(String awardDate) {
        this.awardDate = awardDate;
    }

    public String getRewardId() {
        return rewardId;
    }

    public void setRewardId(String rewardId) {
        this.rewardId = rewardId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEtablishment() {
        return etablishment;
    }

    public void setEtablishment(String etablishment) {
        this.etablishment = etablishment;
    }

    public String getOrganizer() {
        return organizer;
    }

    public void setOrganizer(String organizer) {
        this.organizer = organizer;
    }

    public String getAbbreviation() {
        return abbreviation;
    }

    public void setAbbreviation(String abbreviation) {
        this.abbreviation = abbreviation;
    }

    public String getAwardType() {
        return awardType;
    }

    public void setAwardType(String awardType) {
        this.awardType = awardType;
    }

    public String getAwardClass() {
        return awardClass;
    }

    public void setAwardClass(String awardClass) {
        this.awardClass = awardClass;
    }

    public String getAwardGrade() {
        return awardGrade;
    }

    public void setAwardGrade(String awardGrade) {
        this.awardGrade = awardGrade;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getProjectPerson() {
        return projectPerson;
    }

    public void setProjectPerson(String projectPerson) {
        this.projectPerson = projectPerson;
    }

    public String getProjectOrg() {
        return projectOrg;
    }

    public void setProjectOrg(String projectOrg) {
        this.projectOrg = projectOrg;
    }

    public String getPaper() {
        return paper;
    }

    public void setPaper(String paper) {
        this.paper = paper;
    }

    public String getPaperAuthor() {
        return paperAuthor;
    }

    public void setPaperAuthor(String paperAuthor) {
        this.paperAuthor = paperAuthor;
    }

    public String getPerson() {
        return person;
    }

    public void setPerson(String person) {
        this.person = person;
    }

    public String getAwardOrg() {
        return awardOrg;
    }

    public void setAwardOrg(String awardOrg) {
        this.awardOrg = awardOrg;
    }

    public String getRecommendOrg() {
        return recommendOrg;
    }

    public void setRecommendOrg(String recommendOrg) {
        this.recommendOrg = recommendOrg;
    }


    public String getPlace() {
        return place;
    }

    public void setPlace(String place) {
        this.place = place;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(String lastUpdate) {
        this.lastUpdate = lastUpdate;
    }
}
