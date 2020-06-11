package cn.piflow.bundle

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


class PutMongoDBNsscPaper extends ConfigurableStop{
  override val authorEmail: String = "yit"
  override val description: String = "Put data to mongodb"
  val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  val outportList: List[String] = List(PortEnum.NonePort.toString)

  var ip:String=_
  var port:String=_
  var dataBase:String=_
  var collection:String=_

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark: SparkSession = pec.get[SparkSession]()
    val df: DataFrame = in.read()
    val list = df.schema.fieldNames.toList
    val list1 = List("correspondingAuthor","journal","firstAuthor","author","organization","keyword","conference","reference")
    val list2 = list diff(list1)
    var auhtorList = new util.ArrayList[BasicDBObject]()
    // val firstAuthorList = new util.ArrayList[BasicDBObject]()
    var organizationList = new util.ArrayList[BasicDBObject]()
    var referenceList = new util.ArrayList[BasicDBObject]()
    var reList = new util.ArrayList[String]()
    var document = new Document()
    var corresponding_author =""
    var first_author =""
    var conference =""
    var journal =""
    var author =""
    var reference =""
    var organization =""
    var keyword =""

    val value: RDD[Document] = df.rdd.map(row => {
      //  val correspondingAuthorList = new util.ArrayList[BasicDBObject]()
      auhtorList = new util.ArrayList[BasicDBObject]()
      // val firstAuthorList = new util.ArrayList[BasicDBObject]()
      organizationList = new util.ArrayList[BasicDBObject]()
      referenceList = new util.ArrayList[BasicDBObject]()
      reList = new util.ArrayList[String]()
      document = new Document()

      corresponding_author = row.getAs[String]("correspondingAuthor")
      if (!"null".equals(corresponding_author) && corresponding_author != null && corresponding_author != "null") {
        val json = JSON.parseObject(corresponding_author)
        val firstAuthor = new FirstAuthor()
        firstAuthor.setPersonId(json.getString("personId"))
        firstAuthor.setPersonName(json.getString("personName"))
        document.put("correspondingAuthor", Until.getDBObjFromJavaBean(firstAuthor))
      } else {
        document.put("correspondingAuthor", "null")
      }

       first_author = row.getAs[String]("firstAuthor")
      if (!"null".equals(first_author) && first_author != null && first_author != "null") {
        val json1 = JSON.parseObject(first_author)
        val firstAuthor1 = new FirstAuthor()
        firstAuthor1.setPersonId(json1.getString("personId"))
        firstAuthor1.setPersonName(json1.getString("personName"))
        document.put("firstAuthor", Until.getDBObjFromJavaBean(firstAuthor1))
      } else {
        document.put("firstAuthor", "null")
      }

       conference = row.getAs[String]("conference")
      if (!"null".equals(conference) && conference != null && conference != "null") {
        val json1 = JSON.parseObject(conference)
        val c = new Conference
        c.setConferenceId(json1.getString("conferenceId"))
        c.setConferenceName(json1.getString("conferenceName"))
        document.put("conference", Until.getDBObjFromJavaBean(c))
      } else {
        document.put("conference", "null")
      }


       journal = row.getAs[String]("journal")
      if (!"null".equals(journal) && journal != null && journal != "null") {
        val json1 = JSON.parseObject(journal)
        val j = new Journal
        j.setJournalId(json1.getString("journalId"))
        j.setJournalName(json1.getString("journalName"))
        document.put("journal", Until.getDBObjFromJavaBean(j))
      } else {
        document.put("journal", "null")
      }


       author = row.getAs[String]("author")
      if (!"null".equals(author) && author != null && author != "null") {

        val authorArr = JSON.parseArray(author)
        for (i <- 0 until (authorArr.size())) {
          val nObject = authorArr.getJSONObject(i)
          val a = new Author()
          a.setEmail(nObject.getString("email"))
          a.setPersonId(nObject.getString("personId"))
          a.setPersonName(nObject.getString("personName"))
          a.setIndex(nObject.getString("index"))
          a.setOrganizationId(nObject.getString("organizationId"))
          a.setOrganizationName(nObject.getString("organizationName"))
          auhtorList.add(Until.getDBObjFromJavaBean(a))
        }
        document.put("author", auhtorList)
      } else {
        document.put("author", "null")
      }


       reference = row.getAs[String]("reference")
      if (!"null".equals(reference) && reference != null && reference != "null") {
        val referenceArr = JSON.parseArray(reference)
      for (i <- 0 until (referenceArr.size())) {
        val nObject = referenceArr.getJSONObject(i)
        val a = new Reference()
        a.setReferencePaperId(nObject.getString("referencePaperId"))
        a.setReferencePaperName(nObject.getString("referencePaperName"))
        referenceList.add(Until.getDBObjFromJavaBean(a))
      }
        document.put("reference", referenceList)
    }else{
      document.put("reference", "null")
    }


       organization = row.getAs[String]("organization")
      val organizationArr = JSON.parseArray(organization)
      if(!"null".equals(organization) && organization!=null && organization!="null") {

        for (i <- 0 until (organizationArr.size())) {
          val nObject = organizationArr.getJSONObject(i)
          val organization = new PaperOrganization
          organization.setOrganizationId(nObject.getString("organizationId"))
          organization.setOrganizationName(nObject.getString("organizationName"))
          organization.setIndex(nObject.getString("index"))
          organizationList.add(Until.getDBObjFromJavaBean(organization))
        }
        document.put("organization", organizationList)
      }else{
        document.put("organization", "null")
      }



      keyword = row.getAs[String]("keyword")

      if(!"null".equals(keyword) && keyword!=null && keyword!="null"){
        val reArr = keyword.split(",")
        reArr.foreach(t=>{
          reList.add(t)
        })
        document.put("keyword",reList)
      }else{
        document.put("keyword","null")
      }

      list2.foreach(t=>{
        document.put(t,row.getAs[String](t))
      })


      document
    })
    val str = "mongodb://" + ip + ":" + port + "/" + dataBase + "." + collection
    MongoSpark.save(value,WriteConfig(Map("spark.mongodb.output.uri"->str)))

    /*df.write.options(
      Map("spark.mongodb.output.uri" -> ("mongodb://" + ip + ":" + port + "/" + dataBase + "." + collection))
    )
      .mode("append")
      .format("com.mongodb.spark.sql")
      .save()*/
  }

  override def setProperties(map: Map[String, Any]): Unit = {
    ip = MapUtil.get(map,"ip").asInstanceOf[String]
    port = MapUtil.get(map,"port").asInstanceOf[String]
    dataBase = MapUtil.get(map,"dataBase").asInstanceOf[String]
    collection = MapUtil.get(map,"collection").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {
    var descriptor : List[PropertyDescriptor] = List()

    val ip=new PropertyDescriptor().name("ip").displayName("ip").description("IP address,for example:0.0.0.1").defaultValue("").required(true)
    descriptor = ip :: descriptor
    val port=new PropertyDescriptor().name("port").displayName("port").description("the port").defaultValue("").required(true)
    descriptor = port :: descriptor
    val dataBase=new PropertyDescriptor().name("dataBase").displayName("dataBase").description("data base").defaultValue("").required(true)
    descriptor = dataBase :: descriptor
    val collection=new PropertyDescriptor().name("collection").displayName("collection").description("collection").defaultValue("").required(true)
    descriptor = collection :: descriptor

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
