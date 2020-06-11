package cn.piflow.bundle.bean;

import java.io.Serializable;

/**
 * Created by computer on 2019/10/30.
 */
public class Org implements Serializable {
    String organizationId ;

    String code;

    String name ;

    String alias ;

    String englishName ;

    String types ;

    String isLegalPersonInstitution;

    String legalPerson;

    String establelishDate;
    String creator;

    String head;

    String formerHead;

    String country;

    String address;

    String province;

    String city;

    String district;

    String zipCode;

    String parentOrganization;

    String peopleCount;

    String url;

    String mergedOrganization;
    String splittedOrganization;

    String usedOrganization;

    String source;

    String lastUpdate;

    String intro;

    String contact;

    String subject;

    String subjectRange;

    String picture;
    String language;


    int projectNum;
    int patentNum;
    int monographNum;
    int criterionNum;
    int personNum;
    int resultsTotal;
    double influence;
    int referenceNum;
    int cooperationNum;
    int total;
    int journalPaperNum;
    int cnJournalPaperNum;
    int enJournalPaperNum;
    int conferencePaperNum;


    public int getCooperationNum() {
        return cooperationNum;
    }

    public void setCooperationNum(int cooperationNum) {
        this.cooperationNum = cooperationNum;
    }

    public int getProjectNum() {
        return projectNum;
    }

    public void setProjectNum(int projectNum) {
        this.projectNum = projectNum;
    }

    public int getPatentNum() {
        return patentNum;
    }

    public void setPatentNum(int patentNum) {
        this.patentNum = patentNum;
    }

    public int getMonographNum() {
        return monographNum;
    }

    public void setMonographNum(int monographNum) {
        this.monographNum = monographNum;
    }

    public int getCriterionNum() {
        return criterionNum;
    }

    public void setCriterionNum(int criterionNum) {
        this.criterionNum = criterionNum;
    }

    public int getPersonNum() {
        return personNum;
    }

    public void setPersonNum(int personNum) {
        this.personNum = personNum;
    }

    public int getResultsTotal() {
        return resultsTotal;
    }

    public void setResultsTotal(int resultsTotal) {
        this.resultsTotal = resultsTotal;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public double getInfluence() {
        return influence;
    }

    public void setInfluence(double influence) {
        this.influence = influence;
    }

    public int getReferenceNum() {
        return referenceNum;
    }

    public void setReferenceNum(int referenceNum) {
        this.referenceNum = referenceNum;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getJournalPaperNum() {
        return journalPaperNum;
    }

    public void setJournalPaperNum(int journalPaperNum) {
        this.journalPaperNum = journalPaperNum;
    }

    public int getCnJournalPaperNum() {
        return cnJournalPaperNum;
    }

    public void setCnJournalPaperNum(int cnJournalPaperNum) {
        this.cnJournalPaperNum = cnJournalPaperNum;
    }

    public int getEnJournalPaperNum() {
        return enJournalPaperNum;
    }

    public void setEnJournalPaperNum(int enJournalPaperNum) {
        this.enJournalPaperNum = enJournalPaperNum;
    }

    public int getConferencePaperNum() {
        return conferencePaperNum;
    }

    public void setConferencePaperNum(int conferencePaperNum) {
        this.conferencePaperNum = conferencePaperNum;
    }

    public String getPicture() {
        return picture;
    }

    public void setPicture(String picture) {
        this.picture = picture;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getSubjectRange() {
        return subjectRange;
    }

    public void setSubjectRange(String subjectRange) {
        this.subjectRange = subjectRange;
    }

    public String getOrganizationId() {
        return organizationId;
    }

    public void setOrganizationId(String organizationId) {
        this.organizationId = organizationId;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getEnglishName() {
        return englishName;
    }

    public void setEnglishName(String englishName) {
        this.englishName = englishName;
    }

    public String getTypes() {
        return types;
    }

    public void setTypes(String types) {
        this.types = types;
    }

    public String getIsLegalPersonInstitution() {
        return isLegalPersonInstitution;
    }

    public void setIsLegalPersonInstitution(String isLegalPersonInstitution) {
        this.isLegalPersonInstitution = isLegalPersonInstitution;
    }

    public String getLegalPerson() {
        return legalPerson;
    }

    public void setLegalPerson(String legalPerson) {
        this.legalPerson = legalPerson;
    }

    public String getEstablelishDate() {
        return establelishDate;
    }

    public void setEstablelishDate(String establelishDate) {
        this.establelishDate = establelishDate;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getHead() {
        return head;
    }

    public void setHead(String head) {
        this.head = head;
    }

    public String getFormerHead() {
        return formerHead;
    }

    public void setFormerHead(String formerHead) {
        this.formerHead = formerHead;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public String getZipCode() {
        return zipCode;
    }

    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }

    public String getParentOrganization() {
        return parentOrganization;
    }

    public void setParentOrganization(String parentOrganization) {
        this.parentOrganization = parentOrganization;
    }

    public String getPeopleCount() {
        return peopleCount;
    }

    public void setPeopleCount(String peopleCount) {
        this.peopleCount = peopleCount;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getMergedOrganization() {
        return mergedOrganization;
    }

    public void setMergedOrganization(String mergedOrganization) {
        this.mergedOrganization = mergedOrganization;
    }

    public String getSplittedOrganization() {
        return splittedOrganization;
    }

    public void setSplittedOrganization(String splittedOrganization) {
        this.splittedOrganization = splittedOrganization;
    }

    public String getUsedOrganization() {
        return usedOrganization;
    }

    public void setUsedOrganization(String usedOrganization) {
        this.usedOrganization = usedOrganization;
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

    public String getIntro() {
        return intro;
    }

    public void setIntro(String intro) {
        this.intro = intro;
    }

    public String getContact() {
        return contact;
    }

    public void setContact(String contact) {
        this.contact = contact;
    }
}
