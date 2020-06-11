package cn.piflow.bundle

import cn.piflow.bundle.bean.{ Org}
import cn.piflow.bundle.entity._
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark._


class PutEsNsscOrg extends ConfigurableStop{
  override val authorEmail: String = "yit"
  override val description: String = "Put data to mongodb"
  val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  val outportList: List[String] = List(PortEnum.NonePort.toString)

  var index:String=_
  var indexTypes:String=_
  var port:String=_
  var ip:String=_

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark: SparkSession = pec.get[SparkSession]()
    val df: DataFrame = in.read()

    val  ipAndPort=ip+":"+port


    var organizationId =""

    var code=""

    var name =""

    var alias =""

    var englishName =""

    var types =""

    var isLegalPersonInstitution=""

    var legalPerson: String =""

    var establelishDate: String =""

    var creator: String =""

    var head: String =""

    var formerHead: String =""

    var country: String =""

    var address: String =""

    var province: String =""

    var city: String =""

    var district: String =""

    var zipCode: String =""

    var parentOrganization: String =""

    var peopleCount: String =""

    var url: String =""

    var mergedOrganization: String =""

    var splittedOrganization: String =""

    var usedOrganization: String =""

    var source: String =""

    var lastUpdate: String =""

    var intro: String =""

    var contact: String =""
    var language: String =""

    var picture=""
    var subject=""
    var subjectRange=""
    var projectNum=0
    var patentNum=0
    var monographNum=0
    var criterionNum=0
    var personNum=0
    var resultsTotal=0
    var influence=0d
    var referenceNum=0
    var cooperationNum=0
    var total=0
    var journalPaperNum=0
    var cnJournalPaperNum=0
    var enJournalPaperNum=0
    var conferencePaperNum=0



    val o = new Org
    val personRdd: RDD[Org] = df.rdd.map(t => {

      organizationId = t.getAs[String]("organizationId")
      o.setOrganizationId(organizationId)

      code = t.getAs[String]("code")
      o.setCode(code)

      name = t.getAs[String]("name")
      o.setName(name)

      alias = t.getAs[String]("alias")
      o.setAlias(alias)

      types = t.getAs[String]("type")
      o.setTypes(types)

      isLegalPersonInstitution = t.getAs[String]("isLegalPersonInstitution")
      o.setIsLegalPersonInstitution(isLegalPersonInstitution)

      legalPerson = t.getAs[String]("legalPerson")
      o.setLegalPerson(legalPerson)

      establelishDate = t.getAs[String]("establelishDate")
      o.setEstablelishDate(establelishDate)

      creator = t.getAs[String]("creator")
      o.setCreator(creator)

      head = t.getAs[String]("head")
      o.setHead(head)

      formerHead = t.getAs[String]("formerHead")
      o.setFormerHead(formerHead)

      country = t.getAs[String]("country")
      o.setCountry(country)

      address = t.getAs[String]("address")
      o.setAddress(address)

      province = t.getAs[String]("province")
      o.setProvince(province)

      city = t.getAs[String]("city")
      o.setCity(city)

      district = t.getAs[String]("district")
      o.setDistrict(district)

      zipCode = t.getAs[String]("zipCode")
      o.setZipCode(zipCode)

      parentOrganization = t.getAs[String]("parentOrganization")
      o.setParentOrganization(parentOrganization)

      peopleCount = t.getAs[String]("peopleCount")
      o.setPeopleCount(peopleCount)

      url = t.getAs[String]("url")
      o.setUrl(url)

      mergedOrganization = t.getAs[String]("mergedOrganization")
      o.setMergedOrganization(mergedOrganization)

      splittedOrganization = t.getAs[String]("splittedOrganization")
      o.setSplittedOrganization(splittedOrganization)

      usedOrganization = t.getAs[String]("usedOrganization")
      o.setUsedOrganization(usedOrganization)

      source = t.getAs[String]("source")
      o.setSource(source)

      lastUpdate = t.getAs[String]("lastUpdate")
      o.setLastUpdate(lastUpdate)

      intro = t.getAs[String]("intro")
      o.setIntro(intro)

      contact = t.getAs[String]("contact")
      o.setContact(contact)




      picture = t.getAs[String]("picture")
      o.setPicture(picture)

      subject = t.getAs[String]("subject")
      o.setSubject(subject)

      subjectRange = t.getAs[String]("subjectRange")
      o.setSubjectRange(subjectRange)


      language = t.getAs[String]("language")
      o.setLanguage(language)

      projectNum = t.getAs[Integer]("projectNum")
      o.setProjectNum(projectNum)


      patentNum = t.getAs[Integer]("patentNum")
      o.setPatentNum(patentNum)



      monographNum = t.getAs[Integer]("monographNum")
      o.setMonographNum(monographNum)

      criterionNum = t.getAs[Integer]("criterionNum")
      o.setCriterionNum(criterionNum)

      personNum = t.getAs[Integer]("personNum")
      o.setPersonNum(personNum)

      resultsTotal = t.getAs[Integer]("resultsTotal")
      o.setResultsTotal(resultsTotal)

      influence = t.getAs[Double]("influence")
      o.setInfluence(influence)

      referenceNum = t.getAs[Integer]("referenceNum")
      o.setReferenceNum(referenceNum)

      cooperationNum = t.getAs[Integer]("cooperationNum")
      o.setCooperationNum(cooperationNum)

      total = t.getAs[Integer]("total")
      o.setTotal(total)

      journalPaperNum = t.getAs[Integer]("journalPaperNum")
      o.setJournalPaperNum(journalPaperNum)

      cnJournalPaperNum = t.getAs[Integer]("cnJournalPaperNum")
      o.setCnJournalPaperNum(cnJournalPaperNum)

      enJournalPaperNum = t.getAs[Integer]("enJournalPaperNum")
      o.setEnJournalPaperNum(enJournalPaperNum)

      conferencePaperNum = t.getAs[Integer]("conferencePaperNum")
      o.setConferencePaperNum(conferencePaperNum)



      o
    })

    var map :Map[String,Any]=null
    personRdd.map(t=>{
      map = Map(
        if(t.getOrganizationId!="" && t.getOrganizationId!="null"){
          "organizationId"->t.getOrganizationId
        }else{
          "organizationId"->null
        },

        if(t.getCode!="" && t.getCode!="null"){
          "code"->t.getCode
        }else{
          "code"->null
        },

        if(t.getName!="" && t.getName!="null"){
          "name"->t.getName
        }else{
          "name"->null
        },

        if(t.getAlias!="" && t.getAlias!="null"&& !"null".equals(t.getAlias) && t.getAlias!=null){
          "alias"->t.getAlias.split(" ")
        }else{
          "alias"->null
        },

        if(t.getEnglishName!="" && t.getEnglishName!="null"){
          "englishName"->t.getEnglishName
        }else{
          "englishName"->null
        },

        if(t.getTypes!="" && t.getTypes!="null"){
          "type"->t.getTypes
        }else{
          "type"->null
        },

        if(t.getIsLegalPersonInstitution!="" && t.getIsLegalPersonInstitution!="null"){
          "isLegalPersonInstitution"->t.getIsLegalPersonInstitution
        }else{
          "isLegalPersonInstitution"->null
        },

        if(t.getLegalPerson!="" && t.getLegalPerson!="null"){
          "legalPerson"->t.getLegalPerson
        }else{
          "legalPerson"->null
        },

        if(t.getEstablelishDate!="" && t.getEstablelishDate!="null"){
          "establelishDate"->t.getEstablelishDate
        }else{
          "establelishDate"->null
        },
        if(t.getCreator!="" && t.getCreator!="null" && t.getCreator!=null){
          "creator"->JSON.parseArray(t.getCreator,classOf[FirstAuthor])
        }else{
          "creator"->null
        },

        if(t.getHead!="" && t.getHead!="null" && t.getHead!=null){
          "head"->JSON.parseObject(t.getHead,classOf[FirstAuthor])
        }else{
          "head"->null
        },

        if(t.getFormerHead!="" && t.getFormerHead!="null" && t.getFormerHead!=null){
          "formerHead"->JSON.parseArray(t.getFormerHead,classOf[FormerHead])
        }else{
          "formerHead"->null
        },

        if(t.getCountry!="" && t.getCountry!="null"){
          "country"->t.getCountry
        }else{
          "country"->null
        },

        if(t.getAddress!="" && t.getAddress!="null"){
          "address"->t.getAddress
        }else{
          "address"->null
        },

        if(t.getProvince!="" && t.getProvince!="null"){
          "province"->t.getProvince
        }else{
          "province"->null
        },

        if(t.getCity!="" && t.getCity!="null"){
          "city"->t.getCity
        }else{
          "city"->null
        },

        if(t.getDistrict!="" && t.getDistrict!="null"){
          "district"->t.getDistrict
        }else{
          "district"->null
        },

        if(t.getZipCode!="" && t.getZipCode!="null"){
          "zipCode"->t.getZipCode
        }else{
          "zipCode"->null
        },

        if(t.getParentOrganization!="" && t.getParentOrganization!="null" && t.getParentOrganization!=null){
          if(JSON.parseObject(t.getParentOrganization,classOf[OrganizationNameAndId]).getOrganizationName!=null && JSON.parseObject(t.getParentOrganization,classOf[OrganizationNameAndId]).getOrganizationName!="null"){
            "parentOrganization"->JSON.parseObject(t.getParentOrganization,classOf[OrganizationNameAndId])
          }else{
            "parentOrganization"->null
          }
        }else{
          "parentOrganization"->null
        },

        if(t.getPeopleCount!="" && t.getPeopleCount!="null" &&t.getPeopleCount!=null){
          "peopleCount"->JSON.parseArray(t.getPeopleCount,classOf[PeopleCount])
        }else{
          "peopleCount"->null
        },

        if(t.getUrl!="" && t.getUrl!="null"){
          "url"->t.getUrl
        }else{
          "url"->null
        },

        if(t.getMergedOrganization!="" && t.getMergedOrganization!="null"){
          "mergedOrganization"->t.getMergedOrganization
        }else{
          "mergedOrganization"->null
        },

        if(t.getSplittedOrganization!="" && t.getSplittedOrganization!="null"){
          "splittedOrganization"->t.getSplittedOrganization
        }else{
          "splittedOrganization"->null
        },

        if(t.getUsedOrganization!="" && t.getUsedOrganization!="null"){
          "usedOrganization"->t.getUsedOrganization
        }else{
          "usedOrganization"->null
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

        if(t.getIntro!="" && t.getIntro!="null"){
          "intro"->t.getIntro
        }else{
          "intro"->null
        },

        if(t.getContact!="" && t.getContact!="null"){
          "contact"->t.getContact
        }else{
          "contact"->null
        },

        if(t.getSubject!="" && t.getSubject!="null"){
          "subject"->t.getSubject
        }else{
          "subject"->null
        },

        if(t.getSubjectRange!="" && t.getSubjectRange!="null"){
          "subjectRange"->t.getSubjectRange
        }else{
          "subjectRange"->null
        },

        if(t.getPicture!="" && t.getPicture!="null"){
          "picture"->t.getPicture
        }else{
          "picture"->null
      },
        "language"->t.getLanguage,
        "projectNum"->t.getProjectNum,
        "patentNum"->t.getPatentNum,
        "monographNum"->t.getMonographNum,
        "criterionNum"->t.getCriterionNum,
        "personNum"->t.getPersonNum,
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
    }).saveToEs(index+"/"+indexTypes,Map("es.mapping.id"->"organizationId",
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
    List(StopGroup.NsscGroup.toString)
  }

  override def initialize(ctx: ProcessContext): Unit = { }

}
