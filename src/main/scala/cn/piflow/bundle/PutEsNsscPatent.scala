package cn.piflow.bundle

import java.text.SimpleDateFormat
import java.util.Date

import cn.piflow.bundle.bean.Patent
import cn.piflow.bundle.entity.{Journal, _}
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark._


class PutEsNsscPatent extends ConfigurableStop{
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
    val patent = new Patent

    var patentId=""
    var achievementType=""
    var chineseTitle=""
    var englishTitle=""
    var doi=""
    var authors=""
    var areas =""
    var language=""
    var applyDate=""
    var awardDate =""
    var patentNo=""
    var country=""
    var publishNum=""
    var publishDate=""
    var noticeNum=""
    var noticeDate=""
    var patentee=""
    var ipc=""
    var cpc=""
    var applicant=""
    var applicantAddress=""
    var invent=""
    var obligee=""
    var agent=""
    var agentOrg=""
    var priority=""
    var keywords=""
    var project =""
    var issueUnit=""
    var patentType =""
    var patentStatus=""
    var publishAgency=""
    var hasFullText=""
//    var year=0
    var fullTextUrl=""
    var url=""
    var source=""
    var lastUpdate=""
    var abs =""
    var commonId =""
    var name =""
    var patentBy =""
    var organization =""
    var mainClassNum =""
    var classNum =""
   // var year =""
    val unit: RDD[Patent] = df.rdd.map(t => {
      patentId = t.getAs[String]("patentId")
      patent.setPatentId(patentId)

      chineseTitle = t.getAs[String]("chineseTitle")
      patent.setChineseTitle(chineseTitle)

      englishTitle = t.getAs[String]("englishTitle")
      patent.setEnglishTitle(englishTitle)

      doi = t.getAs[String]("doi")
      patent.setDoi(doi)

      authors = t.getAs[String]("authors")
      patent.setAuthors(authors)

      areas = t.getAs[String]("areas")
      patent.setAreas(areas)

      language = t.getAs[String]("language")
      patent.setLanguage(language)

      applyDate = t.getAs[String]("applyDate")
      patent.setApplyDate(applyDate)

      awardDate = t.getAs[String]("awardDate")
      patent.setAwardDate(awardDate)

      publishDate = t.getAs[String]("publishDate")
      patent.setPublishDate(publishDate)

      patentNo = t.getAs[String]("patentNo")
      patent.setPatentNo(patentNo)

      country = t.getAs[String]("country")
      patent.setCountry(country)

      publishNum = t.getAs[String]("publishNum")
      patent.setPublishNum(publishNum)

      publishDate = t.getAs[String]("publishDate")
      patent.setPublishDate(publishDate)

      noticeNum = t.getAs[String]("noticeNum")
      patent.setNoticeDate(noticeNum)

      noticeDate = t.getAs[String]("noticeDate")
      patent.setNoticeDate(noticeDate)

      patentee = t.getAs[String]("patentee")
      patent.setPatentee(patentee)

      ipc = t.getAs[String]("ipc")
      patent.setIpc(ipc)

      cpc = t.getAs[String]("cpc")
      patent.setCpc(cpc)

      applicant = t.getAs[String]("applicant")
      patent.setApplicant(applicant)

      applicantAddress = t.getAs[String]("applicantAddress")
      patent.setApplicantAddress(applicantAddress)

      invent = t.getAs[String]("invent")
      patent.setInvent(invent)

      obligee = t.getAs[String]("obligee")
      patent.setObligee(obligee)

      agent = t.getAs[String]("agent")
      patent.setAgent(agent)

      agentOrg = t.getAs[String]("agentOrg")
      patent.setAgentOrg(agentOrg)

      priority = t.getAs[String]("priority")
      patent.setPriority(priority)

      keywords = t.getAs[String]("keywords")
      patent.setKeywords(keywords)

      issueUnit = t.getAs[String]("issueUnit")
      patent.setIssueUnit(issueUnit)

      patentType = t.getAs[String]("patentType")
      patent.setPatentType(patentType)

      patentStatus = t.getAs[String]("patentStatus")
      patent.setPatentStatus(patentStatus)

      publishAgency = t.getAs[String]("publishAgency")
      patent.setPublishAgency(publishAgency)

      hasFullText = t.getAs[String]("hasFullText")
      patent.setHasFullText(hasFullText)

      fullTextUrl = t.getAs[String]("fullTextUrl")
      patent.setFullTextUrl(fullTextUrl)



      url = t.getAs[String]("url")
      patent.setUrl(url)

      source = t.getAs[String]("source")
      patent.setSource(source)

      lastUpdate = t.getAs[String]("lastUpdate")
      patent.setLastUpdate(lastUpdate)

      abs = t.getAs[String]("abstract")
      patent.setAbs(abs)

      if(t.getAs[Integer]("year")!=null){
        val year = t.getAs[Integer]("year")
        patent.setYear(year)
      }else{
        patent.setYear(0)
      }




      commonId = t.getAs[String]("commonId")
      patent.setCommonId(commonId)


      name = t.getAs[String]("name")
      patent.setCommonId(name)


      patentBy = t.getAs[String]("patentBy")
      patent.setCommonId(patentBy)

      organization = t.getAs[String]("organization")
      patent.setOrganization(organization)


      mainClassNum = t.getAs[String]("mainClassNum")
      patent.setMainClassNum(mainClassNum)


      classNum = t.getAs[String]("classNum")
      patent.setClassNum(classNum)

      patent
    })


    var map :Map[String,Any]=null
    unit.map(t=>{
      map = Map(
        if(t.getPatentId!="" && t.getPatentId!="null"){
          "patentId"->t.getPatentId
        }else{
          "patentId"->null
        },

        if(t.getAchievementType!="" && t.getAchievementType!="null"){
          "achievementType"->t.getAchievementType
        }else{
          "achievementType"->null
        },

        if(t.getChineseTitle!="" && t.getChineseTitle!="null"){
          "chineseTitle"->t.getChineseTitle
        }else{
          "chineseTitle"->null
        },

        if(t.getEnglishTitle!="" && t.getEnglishTitle!="null"){
          "englishTitle"->t.getEnglishTitle
        }else{
          "englishTitle"->null
        },

        if(t.getDoi!="" && t.getDoi!="null"){
          "doi"->t.getDoi
        }else{
          "doi"->null
        },

        if(t.getAuthors!="" && t.getAuthors!="null"){
          "authors"->JSON.parseArray(t.getAuthors,classOf[AuthorIdAndNameEamil])
        }else{
          "authors"->null
        },

        if(t.getAreas!="" && t.getAreas!="null" && t.getAreas!=null){
          "areas"->t.getAreas.split(",")
        }else{
          "areas"->null
        },


        if(t.getLanguage!="" && t.getLanguage!="null" && t.getLanguage!=null){
          "language"->t.getLanguage
        }else{
          "language"->null
        },

        if(t.getApplyDate!="" && t.getApplyDate!="null" &&t.getApplyDate!=null){
          "applyDate"->t.getApplyDate
        }else{
          "applyDate"->null
        },

        if(t.getAwardDate!="" && t.getAwardDate!="null" && t.getAwardDate!=null){
          "awardDate"->t.getAwardDate
        }else{
          "awardDate"->null
        },

        if(t.getPatentNo!="" && t.getPatentNo!="null"){
          "patentNo"->t.getPatentNo
        }else{
          "patentNo"->null
        },

        if(t.getCountry!="" && t.getCountry!="null"){
          "country"->t.getCountry
        }else{
          "country"->null
        },

        if(t.getPublishNum!="" && t.getPublishNum!="null"){
          "publishNum"->t.getPublishNum
        }else{
          "publishNum"->null
        },


        if(t.getPublishDate!="" && t.getPublishDate!="null" && t.getPublishDate!=null){
          "publishDate"->t.getPublishDate
        }else{
          "publishDate"->null
        },


        if(t.getNoticeNum!="" && t.getNoticeNum!="null" && t.getNoticeNum!=null){
          "noticeNum"->t.getNoticeNum
        }else{
          "noticeNum"->null
        },


        if(t.getNoticeDate!="" && t.getNoticeDate!="null"){
          "noticeDate"->t.getNoticeDate
        }else{
          "noticeDate"->null
        },


        if(t.getPatentee!="" && t.getPatentee!="null"){
          "patentee"->t.getPatentee
        }else{
          "patentee"->null
        },

        if(t.getIpc!="" && t.getIpc!="null"){
          "ipc"->t.getIpc
        }else{
          "ipc"->null
        },

        if(t.getCpc!="" && t.getCpc!="null"){
          "cpc"->t.getCpc
        }else{
          "cpc"->null
        },

        if(t.getApplicant!="" && t.getApplicant!="null"){
          "applicant"->t.getApplicant
        }else{
          "applicant"->null
        },

        if(t.getApplicantAddress!="" && t.getApplicantAddress!="null"){
          "applicantAddress"->t.getApplicantAddress
        }else{
          "applicantAddress"->null
        },

        if(t.getInvent!="" && t.getInvent!="null"){
          "invent"->t.getInvent
        }else{
          "invent"->null
        },

        if(t.getObligee!="" && t.getObligee!="null"){
          "obligee"->t.getObligee
        }else{
          "obligee"->null
        },

        if(t.getAgent!="" && t.getAgent!="null"){
          "agent"->t.getAgent
        }else{
          "agent"->null
        },

        if(t.getAgentOrg!="" && t.getAgentOrg!="null"){
          "agentOrg"->t.getAgentOrg
        }else{
          "agentOrg"->null
        },

        if(t.getOrganization!="" && t.getOrganization!="null"){
          "organization"->JSON.parseObject(t.getOrganization,classOf[OrganizationNameAndId])
        }else{
          "organization"->null
        },

        if(t.getPriority!="" && t.getPriority!="null"){
          "priority"->t.getPriority
        }else{
          "priority"->null
        },

        if(t.getKeywords!="" && t.getKeywords!="null"){
          "keywords"->t.getKeywords
        }else{
          "keywords"->null
        },

        if(t.getProject!="" && t.getProject!="null"){
          "project"->t.getProject
        }else{
          "project"->null
        },

        if(t.getIssueUnit!="" && t.getIssueUnit!="null"){
          "issueUnit"->t.getIssueUnit
        }else{
          "issueUnit"->null
        },

        if(t.getPatentType!="" && t.getPatentType!="null"){
          "patentType"->t.getPatentType
        }else{
          "patentType"->null
        },

        if(t.getPatentStatus!="" && t.getPatentStatus!="null"){
          "patentStatus"->t.getPatentStatus
        }else{
          "patentStatus"->null
        },

        if(t.getPublishAgency!="" && t.getPublishAgency!="null"){
          "publishAgency"->t.getPublishAgency
        }else{
          "publishAgency"->null
        },

        if(t.getHasFullText!="" && t.getHasFullText!="null"){
          "hasFullText"->t.getHasFullText
        }else{
          "hasFullText"->null
        },

        if(t.getFullTextUrl!="" && t.getFullTextUrl!="null"){
          "fullTextUrl"->t.getFullTextUrl
        }else{
          "fullTextUrl"->null
        },


        if(t.getUrl!="" && t.getUrl!="null"){
          "url"->t.getUrl
        }else{
          "url"->null
        },

        if(t.getYear>1949 && t.getYear<=2019){
          "year"->t.getYear.toLong
        }else{
          "year"->null
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

        if(t.getAbs!="" && t.getAbs!="null" && t.getAbs!=null){
          "abstract"->t.getAbs
        }else{
          "abstract"->null
        },

        if(t.getCommonId!="" && t.getCommonId!="null"){
          "commonId"->t.getCommonId
        }else{
          "commonId"->null
        },

        if(t.getName!="" && t.getName!="null"){
          "name"->t.getName
        }else{
          "name"->null
        },

        if(t.getPatentBy!="" && t.getPatentBy!="null"){
          "patentBy"->t.getPatentBy
        }else{
          "patentBy"->null
        },

        if(t.getMainClassNum!="" && t.getMainClassNum!="null" &&t.getMainClassNum!=null){
          "mainClassNum"->t.getMainClassNum
        }else{
          "mainClassNum"->null
        },

        if(t.getClassNum!="" && t.getClassNum!="null" && t.getClassNum!=null){
          "classNum"->t.getClassNum.split(",")
        }else{
          "classNum"->null
        }
      )
      map
    }).saveToEs(index+"/"+indexTypes,Map("es.mapping.id"->"patentId",
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
