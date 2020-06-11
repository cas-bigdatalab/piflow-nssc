package cn.piflow.bundle

import java.text.SimpleDateFormat

import cn.piflow.bundle.bean.Reward
import cn.piflow.bundle.entity._
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.ImageUtil
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark._


class PutEsNsscReward extends ConfigurableStop{
  override val authorEmail: String = "yit"
  override val description: String = "Put data to mongodb"
  val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  val outportList: List[String] = List(PortEnum.NonePort.toString)


  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark: SparkSession = pec.get[SparkSession]()
    val df: DataFrame = in.read()


    val reward = new Reward

    var rewardId=""
    var name=""
    var etablishment=""
    var organizer=""
    var abbreviation=""
    var awardType=""
    var awardClass=""
    var awardGrade=""
    var project=""
    var projectPerson =""
    var projectOrg=""
    var paper=""
    var paperAuthor=""
    var person=""
    var awardOrg=""
    var recommendOrg=""
    var date=""
    var place=""
    var source=""
    var lastUpdate=""
    var commonId=""


    val unit: RDD[Reward] = df.rdd.map(t => {
      rewardId = t.getAs[String]("rewardId")
      reward.setRewardId(rewardId)

      name = t.getAs[String]("name")
      reward.setName(name)

      etablishment = t.getAs[String]("etablishment")
      reward.setEtablishment(etablishment)

      organizer = t.getAs[String]("organizer")
      reward.setOrganizer(organizer)

      abbreviation = t.getAs[String]("abbreviation")
      reward.setAbbreviation(abbreviation)

      awardType = t.getAs[String]("awardType")
      reward.setAwardType(awardType)

      awardClass = t.getAs[String]("awardClass")
      reward.setAwardClass(awardClass)

      awardGrade = t.getAs[String]("awardGrade")
      reward.setAwardGrade(awardGrade)

      project = t.getAs[String]("project")
      reward.setProject(project)

      projectPerson = t.getAs[String]("projectPerson")
      reward.setProjectPerson(projectPerson)

      projectOrg = t.getAs[String]("projectOrg")
      reward.setProjectOrg(projectOrg)

      paper = t.getAs[String]("paper")
      reward.setPaper(paper)

      paperAuthor = t.getAs[String]("paperAuthor")
      reward.setPaperAuthor(paperAuthor)

      person = t.getAs[String]("person")
      reward.setPerson(person)

      awardOrg = t.getAs[String]("awardOrg")
      reward.setAwardOrg(awardOrg)

      recommendOrg = t.getAs[String]("recommendOrg")
      reward.setRecommendOrg(recommendOrg)

      date = t.getAs[String]("awardDate")
      reward.setAwardDate(date)

      place = t.getAs[String]("place")
      reward.setPlace(place)

      source = t.getAs[String]("source")
      reward.setSource(source)

      lastUpdate = t.getAs[String]("lastUpdate")
      reward.setLastUpdate(lastUpdate)


      commonId = t.getAs[String]("commonId")
      reward.setCommonId(commonId)

      if(t.getAs[Integer]("year")!=null){
        val year = t.getAs[Integer]("year")
        reward.setYear(year)
      }else{
        reward.setYear(0)
      }

      reward
    })


    var map :Map[String,Any]=null
    unit.map(t=>{
      map = Map(
        if(t.getRewardId!="" && t.getRewardId!="null"){
          "rewardId"->t.getRewardId
        }else{
          "rewardId"->null
        },

        if(t.getName!="" && t.getName!="null"){
          "name"->t.getName
        }else{
          "name"->null
        },

        if(t.getEtablishment!="" && t.getEtablishment!="null"){
          "etablishment"->t.getEtablishment
        }else{
          "etablishment"->null
        },

        if(t.getOrganizer!="" && t.getOrganizer!="null" && t.getOrganizer!=null){
          "organizer"->t.getOrganizer
        }else{
          "organizer"->null
        },

        if(t.getAbbreviation!="" && t.getAbbreviation!="null"){
          "abbreviation"->t.getAbbreviation
        }else{
          "abbreviation"->null
        },

        if(t.getAwardType!="" && t.getAwardType!="null"){
          "awardType"->t.getAwardType
        }else{
          "awardType"->null
        },


        if(t.getAwardClass!="" && t.getAwardClass!="null"){
          "awardClass"->t.getAwardClass
        }else{
          "awardClass"->null
        },

        if(t.getAwardGrade!="" && t.getAwardGrade!="null"){
          "awardGrade"->t.getAwardGrade
        }else{
          "awardGrade"->null
        },

        if(t.getProject!="" && t.getProject!="null"){
          "project"->t.getProject
        }else{
          "project"->null
        },

        if(t.getProjectPerson!="" && t.getProjectPerson!="null"){
          "projectPerson"->t.getProjectPerson
        }else{
          "projectPerson"->null
        },

        if(t.getProjectOrg!="" && t.getProjectOrg!="null"){
          "projectOrg"->t.getProjectOrg
        }else{
          "projectOrg"->null
        },

        if(t.getPaper!="" && t.getPaper!="null"){
          "paper"->t.getPaper
        }else{
          "paper"->null
        },


        if(t.getPaperAuthor!="" && t.getPaperAuthor!="null"){
          "paperAuthor"->t.getPaperAuthor
        }else{
          "paperAuthor"->null
        },


        if(!"null".equals(t.getPerson) && t.getPerson!=null && t.getPerson!="null"){
          "person"->JSON.parseArray(t.getPerson,classOf[RewardPerson])
        }else{
          "person"->null
        },


        if(!"null".equals(t.getAwardOrg) && t.getAwardOrg!=null && t.getAwardOrg!="null"){
          "awardOrg"->JSON.parseArray(t.getAwardOrg,classOf[OrganizationNameAndId])
        }else{
          "awardOrg"->null
        },


        if(t.getRecommendOrg!="" && t.getRecommendOrg!="null"){
          "recommendOrg"->t.getRecommendOrg
        }else{
          "recommendOrg"->null
        },

        if(t.getAwardDate!="" && t.getAwardDate!="null"){
          "awardDate"->new SimpleDateFormat("yyyy-MM-dd").parse(t.getAwardDate)
        }else{
          "awardDate"->null
        },

        if(t.getPlace!="" && t.getPlace!="null"){
          "place"->t.getPlace
        }else{
          "place"->null
        },

        if(t.getSource!="" && t.getSource!="null" && t.getSource!=null){
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
        },

        if(t.getYear>1949 && t.getYear<2019){
          "year"->t.getYear.toLong
        }else{
          "year"->null
        }
      )
      map
    }).saveToEs("reward_index/reward",Map("es.mapping.id"->"rewardId",
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
    List(StopGroup.NsscGroup.toString)
  }

  override def initialize(ctx: ProcessContext): Unit = { }

}
