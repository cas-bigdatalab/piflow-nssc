package cn.piflow.bundle

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


class PutMongoDBNsscPerson extends ConfigurableStop{
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
    val list1 = List("currentTitle","currentOrganization","workExperience","researchArea")
    val list2 = list diff(list1)
    var current_title =""
    var currentOrganization =""
    var workExperience =""
    var researchArea =""

    var currentOrganizationList = new util.ArrayList[BasicDBObject]()
    var currentTitleList = new util.ArrayList[BasicDBObject]()
    var workExperienceList = new util.ArrayList[BasicDBObject]()
    var reList = new util.ArrayList[String]()

    var document = new Document()


    var curr = new CurrentOrganization

    val value: RDD[Document] = df.rdd.map(row => {
       currentOrganizationList = new util.ArrayList[BasicDBObject]()
       currentTitleList = new util.ArrayList[BasicDBObject]()
       workExperienceList = new util.ArrayList[BasicDBObject]()
       reList = new util.ArrayList[String]()

       document = new Document()

      current_title = row.getAs[String]("currentTitle")
      if(!"null".equals(current_title) && current_title!=null && current_title!="null") {
        val array = JSON.parseArray(current_title)
        for (i <- 0 until (array.size())) {
          val title = new CurrentTitle()
          val nObject = array.getJSONObject(i)
          title.setTitle(nObject.getString("title"))
          title.setOrganizationName(nObject.getString("organizationName"))
          title.setOrganizationId(nObject.getString("organizationId"))
          title.setDate(nObject.getString("date"))
          title.setDepartment(nObject.getString("department"))
          currentTitleList.add(Until.getDBObjFromJavaBean(title))
        }
        document.put("currentTitle", currentTitleList)
      }else{
        document.put("currentTitle", "null")
      }



       currentOrganization = row.getAs[String]("currentOrganization")
      if(!"null".equals(currentOrganization) && currentOrganization!=null && currentOrganization!="null") {
        val json = JSON.parseObject(currentOrganization)
        curr = new CurrentOrganization
        curr.setOrganizationName(json.getString("organizationName"))
        curr.setOrganizationId(json.getString("organizationId"))
        curr.setDepartment(json.getString("department"))
        curr.setType(json.getString("type"))
        currentOrganizationList.add(Until.getDBObjFromJavaBean(curr))
        document.put("currentOrganization", currentOrganizationList)

      }else{
        document.put("currentOrganization", "null")
      }


       workExperience = row.getAs[String]("workExperience")
      if(!"null".equals(workExperience) && workExperience!=null && workExperience!="null") {

        val workExperienceArr = JSON.parseArray(workExperience)
        for (i <- 0 until (workExperienceArr.size())) {
          val nObject = workExperienceArr.getJSONObject(i)
          val c = new WorkExperience
          c.setStartDate(nObject.getString("startDate"))
          c.setEndDate(nObject.getString("endDate"))
          c.setTitle(nObject.getString("title"))
          c.setOrganizationName(nObject.getString("organizationName"))
          c.setOrganizationId(nObject.getString("organizationId"))
          c.setDepartment(nObject.getString("department"))
          workExperienceList.add(Until.getDBObjFromJavaBean(c))
        }
        document.put("workExperience", workExperienceList)
      }else{
        document.put("workExperience", "null")
      }



       researchArea = row.getAs[String]("researchArea")

      if(!"null".equals(researchArea) && researchArea!=null && researchArea!="null"){
        val reArr = researchArea.split("、")
        reArr.foreach(t=>{
          reList.add(t)
        })
        document.put("researchArea",reList)
      }else{
        document.put("researchArea","null")
      }


      list2.foreach(t=>{
        document.put(t,row.getAs[String](t))
      })


      document
    })
    val str = "mongodb://" + ip + ":" + port + "/" + dataBase + "." + collection
    MongoSpark.save(value,WriteConfig(Map("spark.mongodb.output.uri"->str)))

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
