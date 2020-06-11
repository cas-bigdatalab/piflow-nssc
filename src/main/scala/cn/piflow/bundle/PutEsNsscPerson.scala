package cn.piflow.bundle

import cn.piflow.bundle.bean.{Org, Person}
import cn.piflow.bundle.entity._
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark._


class PutEsNsscPerson extends ConfigurableStop{
  override val authorEmail: String = "yit"
  override val description: String = "Put data to mongodb"
  val inportList: List[String] = List(Port.DefaultPort.toString)
  val outportList: List[String] = List(Port.NonePort.toString)


  var index:String=_
  var indexTypes:String=_
  var port:String=_
  var ip:String=_
  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark: SparkSession = pec.get[SparkSession]()
    val df: DataFrame = in.read()



    val ipAndPort=ip+":"+port

    var personId =""

    var idType=""

    var idNo =""

    var orcId =""

    var openId =""

    var chineseName =""

    var englishName=""

    var gender: String =""

    var birthday: String =""

    var ethnicity: String =""

    var birthplace: String =""

    var politicalStatus: String =""

    var isDead: String =""

    var nationality: String =""

    var contact: String =""

    var email: String =""

    var mailAddress: String =""

    var briefDescription: String =""

    var avatar: String =""

    var currentPosition: String =""

    var currentTitle: String =""

    var currentOrganization: String =""

    var workExperience: String =""

    var socialTitle: String =""

    var academicTitle: String =""

    var reviewExpert: String =""

    var researchArea: String =""

    var degree: String =""

    var education: String =""

    var overSeaExperience: String =""

    var isPostdoctoral: String =""

    var postdoctor: String =""

    var language: String =""

    var award: String =""

    var honor: String =""

    var isAcademician: String =""

    var academician: String =""

    var hasTenure: String =""

    var isOutstanding: String =""

    var outstanding: String =""

    var leader: String =""

    var source: String =""

    var lastUpdate: String =""
    var name: String =""

    var organization=""
    var subject=""
    var picture=""


    var projectNum=0
    var patentNum=0
    var monographNum=0
    var criterionNum=0
    var taskNum=0
    var resultsTotal=0
    var influence=0d
    var referenceNum=0
    var cooperationNum=0
    var total=0
    var journalPaperNum=0
    var cnJournalPaperNum=0
    var enJournalPaperNum=0
    var conferencePaperNum=0

    val person = new Person()
    val personRdd: RDD[Person] = df.rdd.map(t => {
      personId = t.getAs[String]("personId")
      person.setPersonId(personId)

      idType = t.getAs[String]("idType")
      person.setIdType(idType)

      idNo = t.getAs[String]("idNo")
      person.setIdNo(idNo)

      orcId = t.getAs[String]("orcId")
      person.setOrcId(orcId)

      openId = t.getAs[String]("openId")
      person.setOpenId(openId)

      chineseName = t.getAs[String]("chineseName")
      person.setChineseName(chineseName)

      englishName = t.getAs[String]("englishName")
      person.setEnglishName(englishName)

      gender = t.getAs[String]("gender")
      person.setGender(gender)

      birthday = t.getAs[String]("birthday")
      person.setBirthday(birthday)

      ethnicity = t.getAs[String]("ethnicity")
      person.setEthnicity(ethnicity)

      birthplace = t.getAs[String]("birthplace")
      person.setBriefDescription(briefDescription)

      politicalStatus = t.getAs[String]("politicalStatus")
      person.setPoliticalStatus(politicalStatus)

      isDead = t.getAs[String]("isDead")
      person.setIsDead(isDead)

      nationality = t.getAs[String]("nationality")
      person.setNationality(nationality)

      contact = t.getAs[String]("contact")
      person.setContact(contact)

      email = t.getAs[String]("email")
      person.setEmail(email)

      mailAddress = t.getAs[String]("mailAddress")
      person.setMailAddress(mailAddress)

      briefDescription = t.getAs[String]("briefDescription")
      person.setBriefDescription(briefDescription)

      avatar = t.getAs[String]("avatar")
      person.setAvatar(avatar)

      currentPosition = t.getAs[String]("currentPosition")
      person.setCurrentPosition(currentPosition)

      currentTitle = t.getAs[String]("currentTitle")
      person.setCurrentTitle(currentTitle)

      currentOrganization = t.getAs[String]("currentOrganization")
      person.setCurrentOrganization(currentOrganization)

      workExperience = t.getAs[String]("workExperience")
      person.setWorkExperience(workExperience)

      socialTitle = t.getAs[String]("socialTitle")
      person.setSocialTitle(socialTitle)

      academicTitle = t.getAs[String]("academicTitle")
      person.setAcademicTitle(academicTitle)

      reviewExpert = t.getAs[String]("reviewExpert")
      person.setReviewExpert(reviewExpert)

      researchArea = t.getAs[String]("researchArea")
      person.setResearchArea(researchArea)

      degree = t.getAs[String]("degree")
      person.setDegree(degree)

      education = t.getAs[String]("education")
      person.setEducation(education)

      overSeaExperience = t.getAs[String]("overSeaExperience")
      person.setOverSeaExperience(overSeaExperience)

      isPostdoctoral = t.getAs[String]("isPostdoctoral")
      person.setIsPostdoctoral(isPostdoctoral)

      postdoctor = t.getAs[String]("postdoctor")
      person.setPostdoctor(postdoctor)

      language = t.getAs[String]("language")
      person.setLanguage(language)

      award = t.getAs[String]("award")
      person.setAward(award)

      honor = t.getAs[String]("honor")
      person.setHonor(honor)

      isAcademician = t.getAs[String]("isAcademician")
      person.setIsAcademician(isAcademician)

      academician = t.getAs[String]("academician")
      person.setAcademician(academician)

      hasTenure = t.getAs[String]("hasTenure")
      person.setHasTenure(hasTenure)

      isOutstanding = t.getAs[String]("isOutstanding")
      person.setIsOutstanding(isOutstanding)

      outstanding = t.getAs[String]("outstanding")
      person.setOutstanding(outstanding)

      leader = t.getAs[String]("leader")
      person.setLeader(leader)

      source = t.getAs[String]("source")
      person.setSource(source)

      lastUpdate = t.getAs[String]("lastUpdate")
      name = t.getAs[String]("name")
      person.setLastUpdate(lastUpdate)
      person.setName(name)

      organization = t.getAs[String]("organization")
      person.setOrganization(organization)


      picture = t.getAs[String]("picture")
      person.setPicture(picture)



      projectNum = t.getAs[Integer]("projectNum")
      person.setProjectNum(projectNum)


      patentNum = t.getAs[Integer]("patentNum")
      person.setPatentNum(patentNum)



      monographNum = t.getAs[Integer]("monographNum")
      person.setMonographNum(monographNum)

      criterionNum = t.getAs[Integer]("criterionNum")
      person.setCriterionNum(criterionNum)

      taskNum = t.getAs[Integer]("taskNum")
      person.setTaskNum(taskNum)

      resultsTotal = t.getAs[Integer]("resultsTotal")
      person.setResultsTotal(resultsTotal)

      influence = t.getAs[Double]("influence")
      person.setInfluence(influence)

      referenceNum = t.getAs[Integer]("referenceNum")
      person.setReferenceNum(referenceNum)

      cooperationNum = t.getAs[Integer]("cooperationNum")
      person.setCooperationNum(cooperationNum)

      total = t.getAs[Integer]("total")
      person.setTotal(total)

      journalPaperNum = t.getAs[Integer]("journalPaperNum")
      person.setJournalPaperNum(journalPaperNum)

      cnJournalPaperNum = t.getAs[Integer]("cnJournalPaperNum")
      person.setCnJournalPaperNum(cnJournalPaperNum)

      enJournalPaperNum = t.getAs[Integer]("enJournalPaperNum")
      person.setEnJournalPaperNum(enJournalPaperNum)

      conferencePaperNum = t.getAs[Integer]("conferencePaperNum")
      person.setConferencePaperNum(conferencePaperNum)


      subject = t.getAs[String]("subject")
      person.setSubject(subject)

      person
    })

    var map :Map[String,Any]=null
    personRdd.map(t=>{
      map = Map(
        if(t.getPersonId!="" && t.getPersonId!="null"){
          "personId"->t.getPersonId
        }else{
          "personId"->null
        },

        if(t.getIdType!="" && t.getIdType!="null"){
          "idType"->t.getIdType
        }else{
          "idType"->null
        },

        if(t.getIdNo!="" && t.getIdNo!="null"){
          "idNo"->t.getIdNo
        }else{
          "idNo"->null
        },

        if(t.getOrcId!="" && t.getOrcId!="null"){
          "orcId"->t.getOrcId
        }else{
          "orcId"->null
        },

        if(t.getOpenId!="" && t.getOpenId!="null"){
          "openId"->t.getOpenId
        }else{
          "openId"->null
        },

        if(t.getChineseName!="" && t.getChineseName!="null"){
          "chineseName"->t.getChineseName
        }else{
          "chineseName"->null
        },

        if(t.getEnglishName!="" && t.getEnglishName!="null"){
          "englishName"->t.getEnglishName
        }else{
          "englishName"->null
        },

        if(t.getGender!="" && t.getGender!="null"){
          "gender"->t.getGender
        }else{
          "gender"->null
        },

        if(t.getBirthday!="" && t.getBirthday!="null"){
          "birthday"->t.getBirthday
        }else{
          "birthday"->null
        },
        if(t.getEthnicity!="" && t.getEthnicity!="null"){
          "ethnicity"->t.getEthnicity
        }else{
          "ethnicity"->null
        },

        if(t.getBirthplace!="" && t.getBirthplace!="null"){
          "birthplace"->t.getBirthplace
        }else{
          "birthplace"->null
        },

        if(t.getPoliticalStatus!="" && t.getPoliticalStatus!="null"){
          "politicalStatus"->t.getPoliticalStatus
        }else{
          "politicalStatus"->null
        },

        if(t.getIsDead!="" && t.getIsDead!="null"){
          "isDead"->t.getIsDead
        }else{
          "isDead"->null
        },

        if(t.getNationality!="" && t.getNationality!="null"){
          "nationality"->t.getNationality
        }else{
          "nationality"->null
        },

        if(t.getContact!="" && t.getContact!="null" && t.getContact!=null){
          "contact"->JSON.parseArray(t.getContact,classOf[Contact])
        }else{
          "contact"->null
        },

        if(t.getEmail!="" && t.getEmail!="null"){
          "email"->t.getEmail
        }else{
          "email"->null
        },

        if(t.getMailAddress!="" && t.getMailAddress!="null"){
          "mailAddress"->t.getMailAddress
        }else{
          "mailAddress"->null
        },

        if(t.getBriefDescription!="" && t.getBriefDescription!="null"){
          "briefDescription"->t.getBriefDescription
        }else{
          "briefDescription"->null
        },

        if(t.getAvatar!="" && t.getAvatar!="null"){
          "avatar"->t.getAvatar
        }else{
          "avatar"->null
        },

        if(t.getCurrentPosition!="" && t.getCurrentPosition!="null" && t.getCurrentPosition!=null){
          "currentPosition"->JSON.parseArray(t.getCurrentPosition,classOf[CurrentTitle])
        }else{
          "currentPosition"->null
        },

        if(t.getCurrentTitle!="" && t.getCurrentTitle!="null" && t.getCurrentTitle!=null){
          "currentTitle"->JSON.parseArray(t.getCurrentTitle,classOf[CurrentTitle])
        }else{
          "currentTitle"->null
        },

        if(t.getCurrentOrganization!="" && t.getCurrentOrganization!="null" && t.getCurrentOrganization!=null){
          "currentOrganization"->JSON.parseObject(t.getCurrentOrganization,classOf[CurrentOrganization])
        }else{
          "currentOrganization"->null
        },

        if(t.getWorkExperience!="" && t.getWorkExperience!="null" && t.getWorkExperience!=null){
          "workExperience"->JSON.parseArray(t.getWorkExperience,classOf[WorkExperience])
        }else{
          "workExperience"->null
        },

        if(t.getSocialTitle!="" && t.getSocialTitle!="null"){
          "socialTitle"->t.getSocialTitle
        }else{
          "socialTitle"->null
        },

        if(t.getAcademicTitle!="" && t.getAcademicTitle!="null" && t.getAcademicTitle!=null){
          "academicTitle"->JSON.parseArray(t.getAcademicTitle,classOf[AcademicTitle])
        }else{
          "academicTitle"->null
        },

        if(t.getReviewExpert!="" && t.getReviewExpert!="null"){
          "reviewExpert"->t.getReviewExpert
        }else{
          "reviewExpert"->null
        },

        if(!"null".equals(researchArea) && t.getResearchArea!="" && t.getResearchArea!="null" && t.getResearchArea!=null){
          "researchArea"->t.getResearchArea.split("、")
        }else{
          "researchArea"->null
        },

        if(t.getDegree!="" && t.getDegree!="null" && t.getDegree!=null){
          "degree"->JSON.parseArray(t.getDegree,classOf[Degree])
        }else{
          "degree"->null
        },

        if(t.getEducation!="" && t.getEducation!="null"){
          "education"->t.getEducation
        }else{
          "education"->null
        },

        if(t.getOverSeaExperience!="" && t.getOverSeaExperience!="null"){
          "overSeaExperience"->t.getOverSeaExperience
        }else{
          "overSeaExperience"->null
        },

        if(t.getIsPostdoctoral!="" && t.getIsPostdoctoral!="null"){
          "isPostdoctoral"->t.getIsPostdoctoral
        }else{
          "isPostdoctoral"->null
        },

        if(t.getPostdoctor!="" && t.getPostdoctor!="null"){
          "postdoctor"->t.getPostdoctor
        }else{
          "postdoctor"->null
        },

        if(t.getLanguage!="" && t.getLanguage!="null"){
          "language"->t.getLanguage
        }else{
          "language"->null
        },

        if(t.getAward!="" && t.getAward!="null"){
          "award"->t.getAward
        }else{
          "award"->null
        },

        if(t.getHonor!="" && t.getHonor!="null"){
          "honor"->t.getHonor
        }else{
          "honor"->null
        },

        if(t.getIsAcademician!="" && t.getIsAcademician!="null"){
          "isAcademician"->t.getIsAcademician
        }else{
          "isAcademician"->null
        },

        if(t.getAcademician!="" && t.getAcademician!="null"){
          "academician"->t.getAcademician
        }else{
          "academician"->null
        },

        if(t.getHasTenure!="" && t.getHasTenure!="null"){
          "hasTenure"->t.getHasTenure
        }else{
          "hasTenure"->null
        },

        if(t.getIsOutstanding!="" && t.getIsOutstanding!="null"){
          "isOutstanding"->t.getIsOutstanding
        }else{
          "isOutstanding"->null
        },

        if(t.getOutstanding!="" && t.getOutstanding!="null" && t.getOutstanding!=null){
          "outstanding"->JSON.parseArray(t.getOutstanding,classOf[Outstandings])
        }else{
          "outstanding"->null
        },

        if(t.getLeader!="" && t.getLeader!="null"){
          "leader"->t.getLeader
        }else{
          "leader"->null
        },

        if(t.getSource!="" && t.getSource!="null"){
          "source"->t.getSource
        }else{
          "source"->null
        },

        if(t.getLastUpdate!="" && t.getLastUpdate!="null"){
          "lastUpdate"->t.getLastUpdate
        }else{
          "lastUpdate"->null
        },

        if(t.getName!="" && t.getName!="null"){
          "name"->t.getName
        }else{
          "name"->null
        },

        if(t.getOrganization!="" && t.getOrganization!="null"){
          "organization"->t.getOrganization
        }else{
          "organization"->null
        },

        if(t.getSubject!="" && t.getSubject!="null" && t.getSubject!=null){
          "subject"->JSON.parseArray(t.getSubject,classOf[Rank])
        }else{
          "subject"->null
        },


        "picture"->t.getPicture,
      "projectNum"->t.getProjectNum,
      "patentNum"->t.getPatentNum,
      "monographNum"->t.getMonographNum,
      "criterionNum"->t.getCriterionNum,
      "taskNum"->t.getTaskNum,
      "resultsTotal"->t.getResultsTotal,
      "influence"->t.getInfluence,
      "referenceNum"->t.getReferenceNum,
      "cooperationNum"->t.getCooperationNum,
      "total"->t.getTotal,
      "journalPaperNum"->t.getJournalPaperNum,
      "cnJournalPaperNum"->t.getCnJournalPaperNum,
      "enJournalPaperNum"->t.getEnJournalPaperNum,
      "conferencePaperNum"->t.getConferencePaperNum
      )
      map
    }).saveToEs(index+"/"+indexTypes,Map("es.mapping.id"->"personId",
      "es.index.auto.create"->"true",
      "es.nodes"->ipAndPort,
      "es.nodes.wan.only"->"true",
      "es.http.timeout"->"100m"
    ))

  }

  override def setProperties(map: Map[String, Any]): Unit = {
    index = MapUtil.get(map,"index").asInstanceOf[String]
    indexTypes = MapUtil.get(map,"indexTypes").asInstanceOf[String]
    ip = MapUtil.get(map,"ip").asInstanceOf[String]
    port = MapUtil.get(map,"port").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {
    var descriptor : List[PropertyDescriptor] = List()

    val index = new PropertyDescriptor().name("index").displayName("index").description("es index").defaultValue("").required(true)
    val indexTypes = new PropertyDescriptor().name("indexTypes").displayName("indexTypes").description("es type").defaultValue("").required(true)
    val ip = new PropertyDescriptor().name("ip").displayName("ip").description("es ip address").defaultValue("").required(true)
    val port = new PropertyDescriptor().name("port").displayName("port").description("es port").defaultValue("").required(true)
    descriptor = index :: descriptor
    descriptor = ip :: descriptor
    descriptor = port :: descriptor
    descriptor = indexTypes :: descriptor

    descriptor
  }

  override def getIcon(): Array[Byte] =  {
    ImageUtil.getImage("icon/nssc/put-mongo-nssc.png")
  }

  override def getGroup(): List[String] = {
    List("")
  }

  override def initialize(ctx: ProcessContext): Unit = { }

}
