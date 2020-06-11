package cn.piflow.bundle

import java.util

import cn.piflow.bundle.bean.Journal
import cn.piflow.bundle.entity._
import cn.piflow.bundle.util.Until
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import com.alibaba.fastjson.JSON
import com.mongodb.BasicDBObject
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document
import org.elasticsearch.spark._


class PutEsNsscJournal extends ConfigurableStop{
  override val authorEmail: String = "yit"
  override val description: String = "Put data to mongodb"
  val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  val outportList: List[String] = List(PortEnum.NonePort.toString)


  var esIndex:String=_
  var indexTypes:String=_
  var port:String=_
  var ip:String=_


  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark: SparkSession = pec.get[SparkSession]()
    val df: DataFrame = in.read()


    val journal = new Journal

    val ipAndPort=ip+":"+port

    var journalId=""
    var name=""
    var englishName=""
    var alias=""
    var discipline=""
    var scope=""
    var cn =""
    var issn=""
    var titlePage=""
    var language =""
    var searchEntrance=""
    var url=""
    var chiefEditor=""
    var seniorEditor=""
    var executiveAssociateEditor=""
    var associateEditor=""
    var editor=""
    var parentJournal=""
    var organization=""
    var founder=""
    var area=""
    var index=""
    var publishCycle=""
    var establishTime=""
    var impactFactor=""
    var rank=""
    var status =""
    var source=""
    var lastUpdate =""



    val unit: RDD[Journal] = df.rdd.map(t => {
      journalId = t.getAs[String]("journalId")
      journal.setJournalId(journalId)

      name = t.getAs[String]("name")
      journal.setName(name)

      englishName = t.getAs[String]("englishName")
      journal.setEnglishName(englishName)

      alias = t.getAs[String]("alias")
      journal.setAlias(alias)

      discipline = t.getAs[String]("discipline")
      journal.setDiscipline(discipline)

      scope = t.getAs[String]("scope")
      journal.setScope(scope)

      cn = t.getAs[String]("cn")
      journal.setCn(cn)

      issn = t.getAs[String]("issn")
      journal.setIssn(issn)

      titlePage = t.getAs[String]("titlePage")
      journal.setTitlePage(titlePage)

      language = t.getAs[String]("language")
      journal.setLanguage(language)

      searchEntrance = t.getAs[String]("searchEntrance")
      journal.setSearchEntrance(searchEntrance)

      url = t.getAs[String]("url")
      journal.setUrl(url)

      chiefEditor = t.getAs[String]("chiefEditor")
      journal.setChiefEditor(chiefEditor)

      seniorEditor = t.getAs[String]("seniorEditor")
      journal.setSeniorEditor(seniorEditor)

      executiveAssociateEditor = t.getAs[String]("executiveAssociateEditor")
      journal.setExecutiveAssociateEditor(executiveAssociateEditor)

      associateEditor = t.getAs[String]("associateEditor")
      journal.setAssociateEditor(associateEditor)

      editor = t.getAs[String]("editor")
      journal.setEditor(editor)

      parentJournal = t.getAs[String]("parentJournal")
      journal.setParentJournal(parentJournal)

      organization = t.getAs[String]("organization")
      journal.setOrganization(organization)

      founder = t.getAs[String]("founder")
      journal.setFounder(founder)

      area = t.getAs[String]("area")
      journal.setArea(area)

      index = t.getAs[String]("index")
      journal.setIndex(index)

      publishCycle = t.getAs[String]("publishCycle")
      journal.setPublishCycle(publishCycle)

      establishTime = t.getAs[String]("establishTime")
      journal.setEstablishTime(establishTime)

      impactFactor = t.getAs[String]("impactFactor")
      journal.setImpactFactor(impactFactor)

      rank = t.getAs[String]("rank")
      journal.setRank(rank)

      status = t.getAs[String]("status")
      journal.setStatus(status)

      source = t.getAs[String]("source")
      journal.setSource(source)

      lastUpdate = t.getAs[String]("lastUpdate")
      journal.setLastUpdate(lastUpdate)

      val weight = t.getAs[Integer]("weight")
      journal.setWeight(weight)

      journal
    })


    var map :Map[String,Any]=null
    unit.map(t=>{
      map = Map(
        if(t.getJournalId!="" && t.getJournalId!="null"){
          "journalId"->t.getJournalId
        }else{
          "journalId"->null
        },

        if(t.getName!="" && t.getName!="null"){
          "name"->t.getName
        }else{
          "name"->null
        },

        if(t.getEnglishName!="" && t.getEnglishName!="null"){
          "englishName"->t.getEnglishName
        }else{
          "englishName"->null
        },

        if(t.getAlias!="" && t.getAlias!="null" && t.getAlias!=null){
          "alias"->t.getAlias.split("\\;")
        }else{
          "alias"->null
        },

        if(t.getDiscipline!="" && t.getDiscipline!="null"){
          "discipline"->t.getDiscipline
        }else{
          "discipline"->null
        },

        if(t.getScope!="" && t.getScope!="null"){
          "scope"->t.getScope
        }else{
          "scope"->null
        },


        if(t.getCn!="" && t.getCn!="null"){
          "cn"->t.getCn
        }else{
          "cn"->null
        },

        if(t.getIssn!="" && t.getIssn!="null"){
          "issn"->t.getIssn
        }else{
          "issn"->null
        },

        if(t.getTitlePage!="" && t.getTitlePage!="null"){
          "titlePage"->t.getTitlePage
        }else{
          "titlePage"->null
        },

        if(t.getLanguage!="" && t.getLanguage!="null"){
          "language"->t.getLanguage
        }else{
          "language"->null
        },

        if(t.getSearchEntrance!="" && t.getSearchEntrance!="null"){
          "searchEntrance"->t.getSearchEntrance
        }else{
          "searchEntrance"->null
        },

        if(t.getUrl!="" && t.getUrl!="null"){
          "url"->t.getUrl
        }else{
          "url"->null
        },


        if(t.getChiefEditor!="" && t.getChiefEditor!="null"){
          "chiefEditor"->t.getChiefEditor
        }else{
          "chiefEditor"->null
        },


        if(t.getSeniorEditor!="" && t.getSeniorEditor!="null"){
          "seniorEditor"->t.getSeniorEditor
        }else{
          "seniorEditor"->null
        },


        if(t.getExecutiveAssociateEditor!="" && t.getExecutiveAssociateEditor!="null"){
          "executiveAssociateEditor"->t.getExecutiveAssociateEditor
        }else{
          "executiveAssociateEditor"->null
        },


        if(t.getAssociateEditor!="" && t.getAssociateEditor!="null"){
          "associateEditor"->t.getAssociateEditor
        }else{
          "associateEditor"->null
        },

        if(t.getEditor!="" && t.getEditor!="null"){
          "editor"->t.getEditor
        }else{
          "editor"->null
        },

        if(t.getParentJournal!="" && t.getParentJournal!="null"){
          "parentJournal"->t.getParentJournal
        }else{
          "parentJournal"->null
        },

        if(t.getOrganization!="" && t.getOrganization!="null" && t.getOrganization!=null){
          "organization"->JSON.parseArray(t.getOrganization,classOf[OrganizationNameAndId])
        }else{
          "organization"->null
        },

        if(t.getFounder!="" && t.getFounder!="null"){
          "founder"->t.getFounder
        }else{
          "founder"->null
        },

        if(t.getArea!="" && t.getArea!="null"){
          "area"->t.getArea
        }else{
          "area"->null
        },

        if(t.getIndex!="" && t.getIndex!="null"){
          "index"->t.getIndex
        }else{
          "index"->null
        },

        if(t.getPublishCycle!="" && t.getPublishCycle!="null"){
          "publishCycle"->t.getPublishCycle
        }else{
          "publishCycle"->null
        },


        if(t.getEstablishTime!="" && t.getEstablishTime!="null"){
          "establishTime"->t.getEstablishTime
        }else{
          "establishTime"->null
        },

        if(t.getImpactFactor!="" && t.getImpactFactor!="null" && t.getImpactFactor!=null){
          "impactFactor"->JSON.parseObject(t.getImpactFactor,classOf[ImpactFactor])
        }else{
          "impactFactor"->null
        },

        if(t.getRank!="" && t.getRank!="null"){
          "rank"->t.getRank
        }else{
          "rank"->null
        },

        if(t.getStatus!="" && t.getStatus!="null"){
          "status"->t.getStatus
        }else{
          "status"->null
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
      "weight"->t.getWeight
      )
      map
    }).saveToEs(esIndex+"/"+indexTypes,Map("es.mapping.id"->"journalId",
      "es.index.auto.create"->"true",
      "es.nodes"->ipAndPort,
      "es.nodes.wan.only"->"true",
      "es.http.timeout"->"100m"
    ))


  }

  override def setProperties(map: Map[String, Any]): Unit = {
    esIndex = MapUtil.get(map,"esIndex").asInstanceOf[String]
    indexTypes = MapUtil.get(map,"indexTypes").asInstanceOf[String]
    ip = MapUtil.get(map,"ip").asInstanceOf[String]
    port = MapUtil.get(map,"port").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {
    var descriptor : List[PropertyDescriptor] = List()

    val index = new PropertyDescriptor().name("esIndex").displayName("esIndex").description("es index").defaultValue("").required(true)
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
    List(StopGroup.NsscGroup.toString)
  }

  override def initialize(ctx: ProcessContext): Unit = { }

}
