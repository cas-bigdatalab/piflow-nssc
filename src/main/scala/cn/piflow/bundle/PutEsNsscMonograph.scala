package cn.piflow.bundle

import java.text.SimpleDateFormat

import cn.piflow.bundle.bean.Monograph
import cn.piflow.bundle.entity._
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.ImageUtil
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark._


class PutEsNsscMonograph extends ConfigurableStop{
  override val authorEmail: String = "yit"
  override val description: String = "Put data to mongodb"
  val inportList: List[String] = List(Port.DefaultPort.toString)
  val outportList: List[String] = List(Port.NonePort.toString)


  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark: SparkSession = pec.get[SparkSession]()
    val df: DataFrame = in.read()


    val monograph = new Monograph

    var monographId=""
    var bookTitle=""
    var bookSeriesName=""
    var doi=""
    var author=""
    var organizationAuthor=""
    var conference=""
    var learn=""
    var editor=""
    var area =""
    var wordCount=""
    var reference=""
    var publisher=""
    var publisherHref=""
    var language=""
    var publishDate=""
    var isbn=""
    var fundProject=""
    var hasFullText=""
    var fullTextUrl=""
    var fullTextPath=""
    var source=""
    var lastUpdate=""
    var name=""
    var commonId=""


    val unit: RDD[Monograph] = df.rdd.map(t => {
      monographId = t.getAs[String]("monographId")
      monograph.setMonographId(monographId)

      bookTitle = t.getAs[String]("bookTitle")
      monograph.setBookTitle(bookTitle)

      bookSeriesName = t.getAs[String]("bookSeriesName")
      monograph.setBookSeriesName(bookSeriesName)

      doi = t.getAs[String]("doi")
      monograph.setDoi(doi)

      author = t.getAs[String]("author")
      monograph.setAuthor(author)

      organizationAuthor = t.getAs[String]("organizationAuthor")
      monograph.setOrganizationAuthor(organizationAuthor)

      conference = t.getAs[String]("conference")
      monograph.setConference(conference)

      learn = t.getAs[String]("learn")
      monograph.setLearn(learn)

      editor = t.getAs[String]("editor")
      monograph.setEditor(editor)

      area = t.getAs[String]("area")
      monograph.setArea(area)

      wordCount = t.getAs[String]("wordCount")
      monograph.setWordCount(wordCount)

      reference = t.getAs[String]("reference")
      monograph.setReference(reference)

      publisher = t.getAs[String]("publisher")
      monograph.setPublisher(publisher)

      publisherHref = t.getAs[String]("publisherHref")
      monograph.setPublisherHref(publisherHref)

      language = t.getAs[String]("language")
      monograph.setLanguage(language)

      isbn = t.getAs[String]("isbn")
      monograph.setIsbn(isbn)

      fundProject = t.getAs[String]("fundProject")
      monograph.setFundProject(fundProject)

      hasFullText = t.getAs[String]("hasFullText")
      monograph.setHasFullText(hasFullText)

      fullTextUrl = t.getAs[String]("fullTextUrl")
      monograph.setFullTextUrl(fullTextUrl)

      fullTextPath = t.getAs[String]("fullTextPath")
      monograph.setFullTextPath(fullTextPath)

      source = t.getAs[String]("source")
      monograph.setSource(source)


      lastUpdate = t.getAs[String]("lastUpdate")
      monograph.setLastUpdate(lastUpdate)


      name = t.getAs[String]("name")
      monograph.setName(name)

      commonId = t.getAs[String]("commonId")
      monograph.setCommonId(commonId)

      if(t.getAs[Integer]("year")!=null){
        val year = t.getAs[Integer]("year")
        monograph.setYear(year)
      }else{
        monograph.setYear(0)
      }

      monograph
    })


    var map :Map[String,Any]=null
    unit.map(t=>{
      map = Map(
        if(t.getMonographId!="" && t.getMonographId!="null"){
          "monographId"->t.getMonographId
        }else{
          "monographId"->null
        },

        if(t.getBookTitle!="" && t.getBookTitle!="null"){
          "bookTitle"->t.getBookTitle
        }else{
          "bookTitle"->null
        },

        if(t.getBookSeriesName!="" && t.getBookSeriesName!="null"){
          "bookSeriesName"->t.getBookSeriesName
        }else{
          "bookSeriesName"->null
        },

        if(t.getDoi!="" && t.getDoi!="null" && t.getDoi!=null){
          "doi"->t.getDoi
        }else{
          "doi"->null
        },

        if(!"null".equals(t.getAuthor) && t.getAuthor!=null && t.getAuthor!="null"){
          "author"->JSON.parseArray(t.getAuthor,classOf[AuthorIdAndNameEamil])
        }else{
          "author"->null
        },

        if(!"null".equals(t.getOrganizationAuthor) && t.getOrganizationAuthor!=null && t.getOrganizationAuthor!="null"){
          "organizationAuthor"->JSON.parseArray(t.getOrganizationAuthor,classOf[OrganizationNameAndId])
        }else{
          "organizationAuthor"->null
        },


        if(t.getConference!="" && t.getConference!="null"){
          "conference"->t.getConference
        }else{
          "conference"->null
        },

        if(t.getLearn!="" && t.getLearn!="null"){
          "learn"->t.getLearn
        }else{
          "learn"->null
        },

        if(t.getEditor!="" && t.getEditor!="null"){
          "editor"->t.getEditor
        }else{
          "editor"->null
        },

        if(t.getArea!="" && t.getArea!="null"){
          "area"->t.getArea
        }else{
          "area"->null
        },

        if(t.getWordCount!="" && t.getWordCount!="null"){
          "wordCount"->t.getWordCount
        }else{
          "wordCount"->null
        },

        if(t.getReference!="" && t.getReference!="null"){
          "reference"->t.getReference
        }else{
          "reference"->null
        },


        if(t.getPublisher!="" && t.getPublisher!="null"){
          "publisher"->t.getPublisher
        }else{
          "publisher"->null
        },


        if(!"null".equals(t.getPublisherHref) && t.getPublisherHref!=null && t.getPublisherHref!="null"){
          "publisherHref"->t.getPublisherHref
        }else{
          "publisherHref"->null
        },


        if(!"null".equals(t.getLanguage) && t.getLanguage!=null && t.getLanguage!="null"){
          "language"->t.getLanguage
        }else{
          "language"->null
        },


        if(t.getPublishDate!="" && t.getPublishDate!="null"){
          "publishDate"->t.getPublishDate
        }else{
          "publishDate"->null
        },

        if(t.getIsbn!="" && t.getIsbn!="null"){
          "isbn"->t.getIsbn
        }else{
          "isbn"->null
        },

        if(t.getFundProject!="" && t.getFundProject!="null"){
          "fundProject"->JSON.parseArray(t.getFundProject,classOf[FundProject])
        }else{
          "fundProject"->null
        },

        if(t.getHasFullText!="" && t.getHasFullText!="null" && t.getHasFullText!=null){
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

        if(t.getName!="" && t.getName!="null"){
          "name"->t.getName
        }else{
          "name"->null
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
    }).saveToEs("monograph_index/monograph",Map("es.mapping.id"->"monographId",
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
