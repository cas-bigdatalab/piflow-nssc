package cn.piflow.bundle

import cn.piflow.bundle.bean.Paper
import cn.piflow.bundle.entity.{Journal, _}
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import com.alibaba.fastjson.JSON

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark._


class PutEsNsscPaper extends ConfigurableStop{
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

    val ipAndPort=ip+":"+port

    val paper = new Paper

    var paperId=""
    var chineseTitle=""
    var englishTitle=""
    var doi=""
    var handle=""
    var firstAuthor=""
    var correspondingAuthor =""
    var author=""
    var organization=""
    var publishDate =""
    var keyword=""
    var include=""
    var reference=""
    var journal=""
    var language=""
    var volume=""
    var issue=""
    var pageStart=""
    var pageEnd=""
    var received=""
    var accepted=""
    var firstOnline=""
    var printIssn=""
    var onlineIssn=""
    var fundProject=""
    var paperAward=""
    var hasFullText=""
    var fullTextUrl =""
    var abs=""
    var citation =""
    var source=""
    var lastUpdate=""
    var name=""
//    var year=0
    var types=""
    var commonId=""
    var conference=""
    var country=""
    var city =""
    var referenceNum=0
    var subject=""

    val unit: RDD[Paper] = df.rdd.map(t => {
      paperId = t.getAs[String]("paperId")
      paper.setPaperId(paperId)

      chineseTitle = t.getAs[String]("chineseTitle")
      paper.setChineseTitle(chineseTitle)

      englishTitle = t.getAs[String]("englishTitle")
      paper.setEnglishTitle(englishTitle)

      doi = t.getAs[String]("doi")
      paper.setDoi(doi)

      handle = t.getAs[String]("handle")
      paper.setHandle(handle)

      firstAuthor = t.getAs[String]("firstAuthor")
      paper.setFirstAuthor(firstAuthor)

      correspondingAuthor = t.getAs[String]("correspondingAuthor")
      paper.setCorrespondingAuthor(correspondingAuthor)

      author = t.getAs[String]("author")
      paper.setAuthor(author)

      organization = t.getAs[String]("organization")
      paper.setOrganization(organization)

      publishDate = t.getAs[String]("publishDate")
      paper.setPublishDate(publishDate)

      keyword = t.getAs[String]("keyword")
      paper.setKeyword(keyword)

      include = t.getAs[String]("include")
      paper.setInclude(include)

      reference = t.getAs[String]("reference")
      paper.setReference(reference)

      journal = t.getAs[String]("journal")
      paper.setJournal(journal)

      language = t.getAs[String]("language")
      paper.setLanguage(language)

      volume = t.getAs[String]("volume")
      paper.setVolume(volume)

      issue = t.getAs[String]("issue")
      paper.setIssue(issue)

      pageStart = t.getAs[String]("pageStart")
      paper.setPageStart(pageStart)

      pageEnd = t.getAs[String]("pageEnd")
      paper.setPageEnd(pageEnd)

      received = t.getAs[String]("received")
      paper.setReceived(received)

      accepted = t.getAs[String]("accepted")
      paper.setAccepted(accepted)

      firstOnline = t.getAs[String]("firstOnline")
      paper.setFirstOnline(firstOnline)

      printIssn = t.getAs[String]("printIssn")
      paper.setPrintIssn(printIssn)

      onlineIssn = t.getAs[String]("onlineIssn")
      paper.setOnlineIssn(onlineIssn)

      fundProject = t.getAs[String]("fundProject")
      paper.setFundProject(fundProject)

      paperAward = t.getAs[String]("paperAward")
      paper.setPaperAward(paperAward)

      hasFullText = t.getAs[String]("hasFullText")
      paper.setHasFullText(hasFullText)

      fullTextUrl = t.getAs[String]("fullTextUrl")
      paper.setFullTextUrl(fullTextUrl)

      abs = t.getAs[String]("abstract")
      paper.setAbs(abs)

      citation = t.getAs[String]("citation")
      paper.setCitation(citation)

      source = t.getAs[String]("source")
      paper.setSource(source)

      lastUpdate = t.getAs[String]("lastUpdate")
      paper.setLastUpdate(lastUpdate)

      name = t.getAs[String]("name")
      paper.setName(name)

      if(t.getAs[Integer]("year")!=null){
        val year = t.getAs[Integer]("year")
        paper.setYear(year)
      }else{
        paper.setYear(0)
      }


      types = t.getAs[String]("type")
      paper.setTypes(types)

      commonId = t.getAs[String]("commonId")
      paper.setCommonId(commonId)

      conference = t.getAs[String]("conference")
      paper.setConference(conference)

      country = t.getAs[String]("country")
      paper.setCountry(country)

      city = t.getAs[String]("city")
      paper.setCity(city)

      referenceNum=t.getAs[Integer]("referenceNum")
      paper.setReferenceNum(referenceNum)


      subject=t.getAs[String]("subject")
      paper.setSubject(subject)


      paper
    })


    var map :Map[String,Any]=null
    unit.map(t=>{
      map = Map(
        if(t.getPaperId!="" && t.getPaperId!="null"){
          "paperId"->t.getPaperId
        }else{
          "paperId"->null
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

        if(t.getHandle!="" && t.getHandle!="null"){
          "handle"->t.getHandle
        }else{
          "handle"->null
        },

        if(t.getFirstAuthor!="" && t.getFirstAuthor!="null" && t.getFirstAuthor!=null){
          "firstAuthor"->JSON.parseObject(t.getFirstAuthor,classOf[FirstAuthor])
        }else{
          "firstAuthor"->null
        },


        if(t.getCorrespondingAuthor!="" && t.getCorrespondingAuthor!="null" && t.getCorrespondingAuthor!=null){
          "correspondingAuthor"->JSON.parseObject(t.getCorrespondingAuthor,classOf[FirstAuthor])
        }else{
          "correspondingAuthor"->null
        },

        if(t.getAuthor!="" && t.getAuthor!="null" &&t.getAuthor!=null){
          "author"->JSON.parseArray(t.getAuthor,classOf[Author])
        }else{
          "author"->null
        },

        if(t.getOrganization!="" && t.getOrganization!="null" && t.getOrganization!=null){
          "organization"->JSON.parseArray(t.getOrganization,classOf[PaperOrganization])
        }else{
          "organization"->null
        },

        if(t.getPublishDate!="" && t.getPublishDate!="null"){
          "publishDate"->t.getPublishDate
        }else{
          "publishDate"->null
        },

        if(t.getKeyword!="" && t.getKeyword!="null" & t.getKeyword!=null){
          "keyword"->t.getKeyword.split("；")
        }else{
          "keyword"->null
        },

        if(t.getInclude!="" && t.getInclude!="null" & t.getInclude!=null){
          "include"->t.getInclude.split(",")
        }else{
          "include"->null
        },


        if(t.getReference!="" && t.getReference!="null" && t.getReference!=null){
          "reference"->JSON.parseArray(t.getReference,classOf[Reference])
        }else{
          "reference"->null
        },


        if(t.getJournal!="" && t.getJournal!="null" && t.getJournal!=null){
          "journal"->JSON.parseObject(t.getJournal,classOf[Journal])
        }else{
          "journal"->null
        },


        if(t.getLanguage!="" && t.getLanguage!="null"){
          "language"->t.getLanguage
        }else{
          "language"->null
        },


        if(t.getVolume!="" && t.getVolume!="null"){
          "volume"->t.getVolume
        }else{
          "volume"->null
        },

        if(t.getIssue!="" && t.getIssue!="null"){
          "issue"->t.getIssue
        }else{
          "issue"->null
        },

        if(t.getPageStart!="" && t.getPageStart!="null"){
          "pageStart"->t.getPageStart
        }else{
          "pageStart"->null
        },

        if(t.getPageEnd!="" && t.getPageEnd!="null"){
          "pageEnd"->t.getPageEnd
        }else{
          "pageEnd"->null
        },

        if(t.getReceived!="" && t.getReceived!="null"){
          "received"->t.getReceived
        }else{
          "received"->null
        },

        if(t.getAccepted!="" && t.getAccepted!="null"){
          "accepted"->t.getAccepted
        }else{
          "accepted"->null
        },

        if(t.getFirstOnline!="" && t.getFirstOnline!="null"){
          "firstOnline"->t.getFirstOnline
        }else{
          "firstOnline"->null
        },

        if(t.getPrintIssn!="" && t.getPrintIssn!="null"){
          "printIssn"->t.getPrintIssn
        }else{
          "printIssn"->null
        },

        if(t.getOnlineIssn!="" && t.getOnlineIssn!="null"){
          "onlineIssn"->t.getOnlineIssn
        }else{
          "onlineIssn"->null
        },

        if(t.getFundProject!="" && t.getFundProject!="null"){
          "fundProject"->t.getFundProject
        }else{
          "fundProject"->null
        },

        if(t.getPaperAward!="" && t.getPaperAward!="null"){
          "paperAward"->t.getPaperAward
        }else{
          "paperAward"->null
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

        if(t.getHasFullText!="" && t.getHasFullText!="null"){
          "hasFullText"->t.getHasFullText
        }else{
          "hasFullText"->null
        },

        if(t.getAbs!="" && t.getAbs!="null"){
          "abstract"->t.getAbs
        }else{
          "abstract"->null
        },

        if(t.getCitation!="" && t.getCitation!="null" && t.getCitation!=null){
          "citation"->t.getCitation.toLong
        }else{
          "citation"->null
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

        if(t.getYear>1949 && t.getYear<=2020){
          "year"->t.getYear.toLong
        }else{
          "year"->null
        },

        if(t.getTypes!="" && t.getTypes!="null"){
          "type"->t.getTypes
        }else{
          "type"->null
        },

        if(t.getCommonId!="" && t.getCommonId!="null"){
          "commonId"->t.getCommonId
        }else{
          "commonId"->null
        },

        if(t.getConference!="" && t.getConference!="null" && t.getConference!=null){
          "conference"->JSON.parseObject(t.getConference,classOf[Conference])
        }else{
          "conference"->null
        },

        if(t.getCountry!="" && t.getCountry!="null"){
          "country"->t.getCountry
        }else{
          "country"->null
        },

        if(t.getCity!="" && t.getCity!="null"){
          "city"->t.getCity
        }else{
          "city"->null
        },


        if(t.getSubject!="" && t.getSubject!="null"){
          "subject"->JSON.parseArray(t.getSubject,classOf[Rank])
        }else{
          "subject"->null
        },


        "referenceNum"->t.getReferenceNum
      )
      map
    }).saveToEs(index+"/"+indexTypes,Map("es.mapping.id"->"paperId",
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
