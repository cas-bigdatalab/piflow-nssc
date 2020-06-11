package cn.piflow.bundle

import java.util

import cn.piflow.bundle.entity._
import cn.piflow.bundle.util.Until
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import com.alibaba.fastjson.JSON
import com.mongodb.BasicDBObject
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document


class PutMongoDBNssc extends ConfigurableStop{
  override val authorEmail: String = "yit"
  override val description: String = "Put data to mongodb"
  val inportList: List[String] = List(Port.DefaultPort.toString)
  val outportList: List[String] = List(Port.NonePort.toString)

  var ip:String=_
  var port:String=_
  var dataBase:String=_
  var collection:String=_

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    /*val spark: SparkSession = pec.get[SparkSession]()
    val df: DataFrame = in.read()
    val list = df.schema.fieldNames.toList
    val list1 = List("currentTitle","contact","currentPosition","currentOrganization","workExperience","academicTitle","degree","outstanding","researchArea")
    val list2 = list diff(list1)

    val value: RDD[Document] = df.rdd.map(row => {
      val currentTitleList = new util.ArrayList[BasicDBObject]()
      val currentPostionList = new util.ArrayList[BasicDBObject]()
      val contactList = new util.ArrayList[BasicDBObject]()
      val currentOrganizationList = new util.ArrayList[BasicDBObject]()
      val degreeList = new util.ArrayList[BasicDBObject]()
      val outstandingList = new util.ArrayList[BasicDBObject]()
      val workExperienceList = new util.ArrayList[BasicDBObject]()
      val academicTitleList = new util.ArrayList[BasicDBObject]()
      val reList = new util.ArrayList[String]()

      val document = new Document()
      val current_title = row.getAs[String]("currentTitle")
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

      val current_position = row.getAs[String]("currentPosition")
      val posArray = JSON.parseArray(current_position)
      for (i <- 0 until (posArray.size())) {
        val title = new CurrentTitle()
        val nObject = posArray.getJSONObject(i)
        title.setTitle(nObject.getString("title"))
        title.setOrganizationName(nObject.getString("organizationName"))
        title.setOrganizationId(nObject.getString("organizationId"))
        title.setDate(nObject.getString("date"))
        title.setDepartment(nObject.getString("department"))
        currentPostionList.add(Until.getDBObjFromJavaBean(title))
      }
      document.put("currentPosition", currentTitleList)

      val contact = row.getAs[String]("contact")
      val contactArr = JSON.parseArray(contact)
      for (i <- 0 until (contactArr.size())) {
        val nObject = contactArr.getJSONObject(i)
        val c= new Contact
        c.setContactType(nObject.getString("contactType"))
        c.setNumber(nObject.getString("number"))
        contactList.add(Until.getDBObjFromJavaBean(c))
      }
      document.put("contact", contactList)

      val currentOrganization = row.getAs[String]("currentOrganization")
      val json = JSON.parseObject(currentOrganization)
        val c= new CurrentOrganization
        c.setOrganizationName(json.getString("organizationName"))
        c.setOrganizationId(json.getString("organizationId"))
        c.setDepartment(json.getString("department"))
        c.setType(json.getString("type"))
        currentOrganizationList.add(Until.getDBObjFromJavaBean(c))
      document.put("currentOrganization", currentOrganizationList)


      val degree = row.getAs[String]("degree")
      val degreeArr = JSON.parseArray(degree)
      for (i <- 0 until (degreeArr.size())) {
        val nObject = degreeArr.getJSONObject(i)
        val c= new Degree
        c.setOrganizationName(nObject.getString("organizationName"))
        c.setOrganizationId(nObject.getString("organizationId"))
        c.setCountry(nObject.getString("country"))
        c.setCountryId(nObject.getString("countryId"))
        c.setDegree(nObject.getString("degree"))
        c.setStartDate(nObject.getString("startDate"))
        c.setEndDate(nObject.getString("endDate"))
        c.setMajor(nObject.getString("major"))
        degreeList.add(Until.getDBObjFromJavaBean(c))
      }
      document.put("degree", degreeList)

      val outstanding = row.getAs[String]("outstanding")
      val outstandingArr = JSON.parseArray(outstanding)
      for (i <- 0 until (outstandingArr.size())) {
        val nObject = outstandingArr.getJSONObject(i)
        val c= new Outstandings
        c.setName(nObject.getString("name"))
        c.setDate(nObject.getString("date"))
        outstandingList.add(Until.getDBObjFromJavaBean(c))
      }
      document.put("outstanding", outstandingList)


      val workExperience = row.getAs[String]("workExperience")
      val workExperienceArr = JSON.parseArray(workExperience)
      for (i <- 0 until (workExperienceArr.size())) {
        val nObject = workExperienceArr.getJSONObject(i)
        val c= new WorkExperience
        c.setStartDate(nObject.getString("startDate"))
        c.setEndDate(nObject.getString("endDate"))
        c.setTitle(nObject.getString("title"))
        c.setOrganizationName(nObject.getString("organizationName"))
        c.setOrganizationId(nObject.getString("organizationId"))
        c.setDepartment(nObject.getString("department"))
        workExperienceList.add(Until.getDBObjFromJavaBean(c))
      }
      document.put("workExperience", workExperienceList)



      val academicTitle = row.getAs[String]("academicTitle")
      val academicTitleArr = JSON.parseArray(academicTitle)
      for (i <- 0 until (academicTitleArr.size())) {
        val nObject = academicTitleArr.getJSONObject(i)
        val c= new AcademicTitle
        c.setStartDate(nObject.getString("startDate"))
        c.setEndDate(nObject.getString("endDate"))
        c.setTitle(nObject.getString("title"))
        c.setOrganizationName(nObject.getString("organizationName"))
        c.setOrganizationId(nObject.getString("organizationId"))
        c.setType(nObject.getString("type"))
        academicTitleList.add(Until.getDBObjFromJavaBean(c))
      }
      document.put("academicTitle", academicTitleList)


      val researchArea = row.getAs[String]("researchArea")
      val reArr = researchArea.split("、")
      reArr.foreach(t=>{
        reList.add(t)
      })
      document.put("researchArea",reList)

      list2.foreach(t=>{
        document.put(t,row.getAs[String](t))
      })



      document
    })
    import com.mongodb.spark._
    val str = "mongodb://" + ip + ":" + port + "/" + dataBase + "." + collection
    MongoSpark.save(value,WriteConfig(Map("spark.mongodb.output.uri"->str)))
*/
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
    List("")
  }

  override def initialize(ctx: ProcessContext): Unit = { }

}
