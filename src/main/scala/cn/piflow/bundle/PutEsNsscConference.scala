package cn.piflow.bundle

import java.text.SimpleDateFormat

import cn.piflow.bundle.bean.Conference
import cn.piflow.bundle.entity._
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.ImageUtil
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark._


class PutEsNsscConference extends ConfigurableStop{
  override val authorEmail: String = "yit"
  override val description: String = "Put data to mongodb"
  val inportList: List[String] = List(Port.DefaultPort.toString)
  val outportList: List[String] = List(Port.NonePort.toString)


  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark: SparkSession = pec.get[SparkSession]()
    val df: DataFrame = in.read()


    val conference = new Conference

    var conferenceId=""
    var name=""
    var abbreviation=""
    var coDate=""
    var place1=""
    var place2=""
    var language =""
    var area=""
    var subject=""
    var organizer =""
    var undertaker=""
    var coOrganizer=""
    var sponsor=""
    var chairman=""
    var viceChairman=""
    var council=""
    var isContinuousConference=""
    var lastConference=""
    var nextConference=""
    var session=""
    var participantCount=""
    var rank=""
    var url=""
    var hasSummaryset=""
    var summerysetPath=""
    var hasProceeding=""
    var proceedingPath =""
    var paperCount=""
    var source =""
    var lastUpdate=""
    var commonId=""
   


    val unit: RDD[Conference] = df.rdd.map(t => {
      conferenceId = t.getAs[String]("conferenceId")
      conference.setConferenceId(conferenceId)

      name = t.getAs[String]("name")
      conference.setName(name)

      abbreviation = t.getAs[String]("abbreviation")
      conference.setAbbreviation(abbreviation)

      coDate = t.getAs[String]("coDate")
      conference.setCoDate(coDate)

      place1 = t.getAs[String]("place1")
      conference.setPlace1(place1)

      place2 = t.getAs[String]("place2")
      conference.setPlace2(place2)

      language = t.getAs[String]("language")
      conference.setLanguage(language)

      area = t.getAs[String]("area")
      conference.setArea(area)

      subject = t.getAs[String]("subject")
      conference.setSubject(subject)

      area = t.getAs[String]("area")
      conference.setArea(area)

      organizer = t.getAs[String]("organizer")
      conference.setOrganizer(organizer)

      undertaker = t.getAs[String]("undertaker")
      conference.setUndertaker(undertaker)

      coOrganizer = t.getAs[String]("coOrganizer")
      conference.setCoOrganizer(coOrganizer)

      language = t.getAs[String]("language")
      conference.setLanguage(language)

      sponsor = t.getAs[String]("sponsor")
      conference.setSponsor(sponsor)

      chairman = t.getAs[String]("chairman")
      conference.setChairman(chairman)

      viceChairman = t.getAs[String]("viceChairman")
      conference.setViceChairman(viceChairman)

      council = t.getAs[String]("council")
      conference.setCouncil(council)

      isContinuousConference = t.getAs[String]("isContinuousConference")
      conference.setIsContinuousConference(isContinuousConference)

      lastConference = t.getAs[String]("lastConference")
      conference.setLastConference(lastConference)

      nextConference = t.getAs[String]("nextConference")
      conference.setNextConference(nextConference)

      session = t.getAs[String]("session")
      conference.setSession(session)

      participantCount = t.getAs[String]("participantCount")
      conference.setParticipantCount(participantCount)

      rank = t.getAs[String]("rank")
      conference.setRank(rank)

      url = t.getAs[String]("url")
      conference.setUrl(url)

      hasSummaryset = t.getAs[String]("hasSummaryset")
      conference.setHasSummaryset(hasSummaryset)

      summerysetPath = t.getAs[String]("summerysetPath")
      conference.setSummerysetPath(summerysetPath)

      hasProceeding = t.getAs[String]("hasProceeding")
      conference.setHasProceeding(hasProceeding)

      proceedingPath = t.getAs[String]("proceedingPath")
      conference.setProceedingPath(proceedingPath)

      paperCount = t.getAs[String]("paperCount")
      conference.setPaperCount(paperCount)

      source = t.getAs[String]("source")
      conference.setSource(source)

      lastUpdate = t.getAs[String]("lastUpdate")
      conference.setLastUpdate(lastUpdate)

      conference
    })


    var map :Map[String,Any]=null
    unit.map(t=>{
      map = Map(
        if(t.getConferenceId!="" && t.getConferenceId!="null"){
          "conferenceId"->t.getConferenceId
        }else{
          "conferenceId"->null
        },

        if(t.getName!="" && t.getName!="null"){
          "name"->t.getName
        }else{
          "name"->null
        },

        if(t.getAbbreviation!="" && t.getAbbreviation!="null"){
          "abbreviation"->t.getAbbreviation
        }else{
          "abbreviation"->null
        },

        if(t.getCoDate!="" && t.getCoDate!="null"){
          "coDate"->t.getCoDate
        }else{
          "coDate"->null
        },

        if(t.getPlace1!="" && t.getPlace1!="null"){
          "place1"->t.getPlace1
        }else{
          "place1"->null
        },

        if(t.getPlace2!="" && t.getPlace2!="null" && t.getPlace2!=null){
          "place2"->t.getPlace2
        }else{
          "place2"->null
        },


        if(t.getLanguage!="" && t.getLanguage!="null" && t.getLanguage!=null){
          "language"->t.getLanguage
        }else{
          "language"->null
        },

        if(t.getArea!="" && t.getArea!="null" &&t.getArea!=null){
          "area"->t.getArea
        }else{
          "area"->null
        },

        if(t.getSubject!="" && t.getSubject!="null" && t.getSubject!=null){
          "subject"->t.getSubject
        }else{
          "subject"->null
        },

        if(t.getOrganizer!="" && t.getOrganizer!="null"){
          "organizer"->t.getOrganizer
        }else{
          "organizer"->null
        },

        if(t.getUndertaker!="" && t.getUndertaker!="null"){
          "undertaker"->t.getUndertaker
        }else{
          "undertaker"->null
        },

        if(t.getCoOrganizer!="" && t.getCoOrganizer!="null"){
          "coOrganizer"->t.getCoOrganizer
        }else{
          "coOrganizer"->null
        },


        if(t.getSponsor!="" && t.getSponsor!="null" && t.getSponsor!=null){
          "sponsor"->t.getSponsor
        }else{
          "sponsor"->null
        },


        if(t.getChairman!="" && t.getChairman!="null" && t.getChairman!=null){
          "chairman"->t.getChairman
        }else{
          "chairman"->null
        },



        if(t.getViceChairman!="" && t.getViceChairman!="null"){
          "viceChairman"->t.getViceChairman
        }else{
          "viceChairman"->null
        },

        if(t.getCouncil!="" && t.getCouncil!="null"){
          "council"->t.getCouncil
        }else{
          "council"->null
        },

        if(t.getIsContinuousConference!="" && t.getIsContinuousConference!="null" && t.getIsContinuousConference!=null){
          "isContinuousConference"->t.getIsContinuousConference
        }else{
          "isContinuousConference"->null
        },

        if(t.getLastConference!="" && t.getLastConference!="null" && t.getLastConference!=null){
          "lastConference"->t.getLastConference
        }else{
          "lastConference"->null
        },

        if(t.getNextConference!="" && t.getNextConference!="null" && t.getNextConference!=null){
          "nextConference"->t.getNextConference
        }else{
          "nextConference"->null
        },

        if(t.getSession!="" && t.getSession!="null" && t.getSession!=null){
          "session"->t.getSession
        }else{
          "session"->null
        },

        if(t.getParticipantCount!="" && t.getParticipantCount!="null" && t.getParticipantCount!=null){
          "participantCount"->t.getParticipantCount
        }else{
          "participantCount"->null
        },

        if(t.getRank!="" && t.getRank!="null" && t.getRank!=null){
          "rank"->t.getRank
        }else{
          "rank"->null
        },

        if(t.getUrl!="" && t.getUrl!="null"){
          "url"->t.getUrl
        }else{
          "url"->null
        },

        if(t.getHasSummaryset!="" && t.getHasSummaryset!="null"){
          "hasSummaryset"->t.getHasSummaryset
        }else{
          "hasSummaryset"->null
        },

        if(t.getSummerysetPath!="" && t.getSummerysetPath!="null"){
          "summerysetPath"->t.getSummerysetPath
        }else{
          "summerysethasProceedingPath"->null
        },

        if(t.getHasProceeding!="" && t.getHasProceeding!="null"){
          "hasProceeding"->t.getHasProceeding
        }else{
          "hasProceeding"->null
        },

        if(t.getProceedingPath!="" && t.getProceedingPath!="null"){
          "proceedingPath"->t.getProceedingPath
        }else{
          "proceedingPath"->null
        },

        if(t.getPaperCount!="" && t.getPaperCount!="null"){
          "paperCount"->t.getPaperCount
        }else{
          "paperCount"->null
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

        if(t.getCommonId!="" && t.getCommonId!="null"){
          "commonId"->t.getCommonId
        }else{
          "commonId"->null
        }

      )
      map
    }).saveToEs("conference_index/conference",Map("es.mapping.id"->"conferenceId",
      "es.index.auto.create"->"true",
      "es.nodes"->"10.0.88.159:9200",
      "es.nodes.wan.only"->"true",
      "es.http.timeout"->"100m"
    ))

  }

  override def setProperties(map: Map[String, Any]): Unit = {

  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {
    var descriptor : List[PropertyDescriptor] = List()


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
