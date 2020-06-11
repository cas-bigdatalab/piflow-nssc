package cn.piflow.bundle

import java.text.SimpleDateFormat

import org.elasticsearch.spark._
import cn.piflow.bundle.bean.Criterion
import cn.piflow.bundle.entity.OrganizationNameAndId
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.ImageUtil
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import cn.piflow.conf.{ConfigurableStop, Port}
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


class PutEsNsscCriterion extends ConfigurableStop{
  override val authorEmail: String = "yit"
  override val description: String = "Put data to mongodb"
  val inportList: List[String] = List(Port.DefaultPort.toString)
  val outportList: List[String] = List(Port.NonePort.toString)


  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark: SparkSession = pec.get[SparkSession]()
    val df: DataFrame = in.read()


    val criterion = new Criterion

    var criterionId=""
    var chineseTitle=""
    var englishTitle=""
    var doi=""
    var criterionNo=""
    var criterionGrade=""
    var criterionType =""
    var mainLanguage=""
    var author=""
    var area =""
    var chineseCode=""
    var internationalCode=""
    var militaryCode=""
    var language=""
    var state=""
    var beReplaceCode=""
    var implementDate=""
    var publishDate=""
    var chargeOrg=""
    var centralizedOrg=""
    var publishOrg=""
    var draftOrg=""
    var applyDate=""
    var awardDate=""
    var appliedNo=""
    var country =""
    var applicant=""
    var publishAgency =""
    var hasFullText=""
    var fullTextUrl=""
    var fullTextPath=""
    var source=""
    var lastUpdate=""
    var commonId=""
    var name=""
    var subject=""


    val unit: RDD[Criterion] = df.rdd.map(t => {
      criterionId = t.getAs[String]("criterionId")
      criterion.setCriterionId(criterionId)

      chineseTitle = t.getAs[String]("chineseTitle")
      criterion.setChineseTitle(chineseTitle)

      englishTitle = t.getAs[String]("englishTitle")
      criterion.setEnglishTitle(englishTitle)

      doi = t.getAs[String]("doi")
      criterion.setDoi(doi)

      criterionNo = t.getAs[String]("criterionNo")
      criterion.setCriterionNo(criterionNo)

      criterionGrade = t.getAs[String]("criterionGrade")
      criterion.setCriterionGrade(criterionGrade)

      criterionType = t.getAs[String]("criterionType")
      criterion.setCriterionType(criterionType)

      mainLanguage = t.getAs[String]("mainLanguage")
      criterion.setMainLanguage(mainLanguage)

      author = t.getAs[String]("author")
      criterion.setAuthor(author)

      area = t.getAs[String]("area")
      criterion.setArea(area)

      chineseCode = t.getAs[String]("chineseCode")
      criterion.setChineseCode(chineseCode)

      internationalCode = t.getAs[String]("internationalCode")
      criterion.setInternationalCode(internationalCode)

      militaryCode = t.getAs[String]("militaryCode")
      criterion.setMilitaryCode(militaryCode)

      language = t.getAs[String]("language")
      criterion.setLanguage(language)

      state = t.getAs[String]("state")
      criterion.setState(state)

      beReplaceCode = t.getAs[String]("beReplaceCode")
      criterion.setBeReplaceCode(beReplaceCode)

      implementDate = t.getAs[String]("implementDate")
      criterion.setImplementDate(implementDate)

      publishDate = t.getAs[String]("publishDate")
      criterion.setPublishDate(publishDate)

      chargeOrg = t.getAs[String]("chargeOrg")
      criterion.setChargeOrg(chargeOrg)

      centralizedOrg = t.getAs[String]("centralizedOrg")
      criterion.setCentralizedOrg(centralizedOrg)

      publishOrg = t.getAs[String]("publishOrg")
      criterion.setPublishOrg(publishOrg)

      draftOrg = t.getAs[String]("draftOrg")
      criterion.setDraftOrg(draftOrg)

      applyDate = t.getAs[String]("applyDate")
      criterion.setApplyDate(applyDate)

      awardDate = t.getAs[String]("awardDate")
      criterion.setAwardDate(awardDate)

      appliedNo = t.getAs[String]("appliedNo")
      criterion.setAppliedNo(appliedNo)

      country = t.getAs[String]("country")
      criterion.setCountry(country)

      applicant = t.getAs[String]("applicant")
      criterion.setApplicant(applicant)

      publishAgency = t.getAs[String]("publishAgency")
      criterion.setPublishAgency(publishAgency)

      hasFullText = t.getAs[String]("hasFullText")
      criterion.setHasFullText(hasFullText)

      fullTextUrl = t.getAs[String]("fullTextUrl")
      criterion.setFullTextPath(fullTextUrl)

      fullTextPath = t.getAs[String]("fullTextPath")
      criterion.setFullTextUrl(fullTextPath)

      lastUpdate = t.getAs[String]("lastUpdate")
      criterion.setLastUpdate(lastUpdate)

      source = t.getAs[String]("source")
      criterion.setSource(source)


      lastUpdate = t.getAs[String]("lastUpdate")
      criterion.setLastUpdate(lastUpdate)

      commonId = t.getAs[String]("commonId")
      criterion.setCommonId(commonId)

      name = t.getAs[String]("name")
      criterion.setName(name)

      subject = t.getAs[String]("subject")
      criterion.setSubject(subject)


      if(t.getAs[Integer]("year")!=null){
        val year = t.getAs[Integer]("year")
        criterion.setYear(year)
      }else{
        criterion.setYear(0)
      }


      criterion
    })


    var map :Map[String,Any]=null


    unit.map(t=>{
      map = Map(
        if(t.getCriterionId!="" && t.getCriterionId!="null"){
          "criterionId"->t.getCriterionId
        }else{
          "criterionId"->null
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

        if(t.getCriterionNo!="" && t.getCriterionNo!="null"){
          "criterionNo"->t.getCriterionNo
        }else{
          "criterionNo"->null
        },

        if(t.getCriterionGrade!="" && t.getCriterionGrade!="null" && t.getCriterionGrade!=null){
          "criterionGrade"->t.getCriterionGrade
        }else{
          "criterionGrade"->null
        },


        if(t.getCriterionType!="" && t.getCriterionType!="null" && t.getCriterionType!=null){
          "criterionType"->t.getCriterionType
        }else{
          "criterionType"->null
        },

        if(t.getMainLanguage!="" && t.getMainLanguage!="null" &&t.getMainLanguage!=null){
          "mainLanguage"->t.getMainLanguage
        }else{
          "mainLanguage"->null
        },

        if(t.getAuthor!="" && t.getAuthor!="null" && t.getAuthor!=null){
         // "author"->JSON.parseArray(t.getAuthor,classOf[AuthorIdAndNameEamil])
          "author"->t.getAuthor
        }else{
          "author"->null
        },

        if(t.getArea!="" && t.getArea!="null"){
          "area"->t.getArea
        }else{
          "area"->null
        },

        if(t.getChineseCode!="" && t.getChineseCode!="null"){
          "chineseCode"->t.getChineseCode
        }else{
          "chineseCode"->null
        },

        if(t.getInternationalCode!="" && t.getInternationalCode!="null"){
          "internationalCode"->t.getInternationalCode
        }else{
          "internationalCode"->null
        },


        if(t.getMilitaryCode!="" && t.getMilitaryCode!="null" && t.getMilitaryCode!=null){
          "militaryCode"->t.getMilitaryCode
        }else{
          "militaryCode"->null
        },


        if(t.getLanguage!="" && t.getLanguage!="null" && t.getLanguage!=null){
          "language"->t.getLanguage
        }else{
          "language"->null
        },



        if(t.getState!="" && t.getState!="null"){
          "state"->t.getState
        }else{
          "state"->null
        },

        if(t.getBeReplaceCode!="" && t.getBeReplaceCode!="null"){
          "beReplaceCode"->t.getBeReplaceCode
        }else{
          "beReplaceCode"->null
        },

        if(t.getImplementDate!="" && t.getImplementDate!="null" && t.getImplementDate!=null){
          "implementDate"->new SimpleDateFormat("yyyyMMdd").parse(t.getImplementDate)
        }else{
          "implementDate"->null
        },

        if(t.getPublishDate!="" && t.getPublishDate!="null" && t.getPublishDate!=null){
          "publishDate"->new SimpleDateFormat("yyyyMMdd").parse(t.getPublishDate)
        }else{
          "publishDate"->null
        },

        if(t.getChargeOrg!="" && t.getChargeOrg!="null" && t.getChargeOrg!=null){
          "chargeOrg"->JSON.parseObject(t.getChargeOrg,classOf[OrganizationNameAndId])
        }else{
          "chargeOrg"->null
        },

        if(t.getCentralizedOrg!="" && t.getCentralizedOrg!="null" && t.getCentralizedOrg!=null){
          "centralizedOrg"->JSON.parseObject(t.getCentralizedOrg,classOf[OrganizationNameAndId])
        }else{
          "centralizedOrg"->null
        },

        if(t.getPublishOrg!="" && t.getPublishOrg!="null" && t.getPublishOrg!=null){
          "publishOrg"->JSON.parseObject(t.getPublishOrg,classOf[OrganizationNameAndId])
        }else{
          "publishOrg"->null
        },

        if(t.getDraftOrg!="" && t.getDraftOrg!="null" && t.getDraftOrg!=null){
          "draftOrg"->JSON.parseArray(t.getDraftOrg,classOf[OrganizationNameAndId])
        }else{
          "draftOrg"->null
        },

        if(t.getApplyDate!="" && t.getApplyDate!="null"){
          "applyDate"->t.getApplyDate
        }else{
          "applyDate"->null
        },

        if(t.getAwardDate!="" && t.getAwardDate!="null"){
          "awardDate"->t.getAwardDate
        }else{
          "awardDate"->null
        },

        if(t.getAppliedNo!="" && t.getAppliedNo!="null"){
          "appliedNo"->t.getAppliedNo
        }else{
          "appliedNo"->null
        },

        if(t.getCountry!="" && t.getCountry!="null"){
          "country"->t.getCountry
        }else{
          "country"->null
        },

        if(t.getApplicant!="" && t.getApplicant!="null"){
          "applicant"->t.getApplicant
        }else{
          "applicant"->null
        },

        if(t.getPublishAgency!="" && t.getPublishAgency!="null"){
          "publishAgency"->JSON.parseArray(t.getPublishAgency,classOf[OrganizationNameAndId])
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

        if(t.getFullTextPath!="" && t.getFullTextPath!="null"){
          "fullTextPath"->t.getFullTextPath
        }else{
          "fullTextPath"->null
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
        },

        if(t.getName!="" && t.getName!="null" && t.getName!=null){
          "name"->t.getName
        }else{
          "name"->null
        },

        if(t.getSubject!="" && t.getSubject!="null"){
          "subject"->t.getSubject
        }else{
          "subject"->null
        },

        if(t.getYear>1949 && t.getYear<2019){
          "year"->t.getYear.toLong
        }else{
          "year"->null
        }
      )
      map
    }).saveToEs("criterion_index/criterion",Map("es.mapping.id"->"criterionId",
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
