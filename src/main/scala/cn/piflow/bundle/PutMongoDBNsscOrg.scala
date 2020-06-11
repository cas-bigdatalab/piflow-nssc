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


class PutMongoDBNsscOrg extends ConfigurableStop{
  override val authorEmail: String = "yit"
  override val description: String = "Put data to mongodb"
  val inportList: List[String] = List(Port.DefaultPort.toString)
  val outportList: List[String] = List(Port.NonePort.toString)

  var ip:String=_
  var port:String=_
  var dataBase:String=_
  var collection:String=_

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
   /* val spark: SparkSession = pec.get[SparkSession]()
    val df: DataFrame = in.read()
    val list = df.schema.fieldNames.toList
    val list1 = List("creator","head","formerHead","parentOrganization","peopleCount")
    val list2 = list diff(list1)

    val value: RDD[Document] = df.rdd.map(row => {
      val creatorList = new util.ArrayList[BasicDBObject]()
     // val headList = new util.ArrayList[BasicDBObject]()
      val formerHeadList = new util.ArrayList[BasicDBObject]()
      val peopleCountList = new util.ArrayList[BasicDBObject]()
      /*val degreeList = new util.ArrayList[BasicDBObject]()
      val outstandingList = new util.ArrayList[BasicDBObject]()
      val workExperienceList = new util.ArrayList[BasicDBObject]()
      val academicTitleList = new util.ArrayList[BasicDBObject]()
      val reList = new util.ArrayList[String]()*/

      val document = new Document()


      val creator = row.getAs[String]("creator")
      if(!"null".equals(creator) && creator!=null && creator!="null") {
        val creatorArr = JSON.parseArray(creator)
        for (i <- 0 until (creatorArr.size())) {
          val nObject = creatorArr.getJSONObject(i)
          val c = new FirstAuthor
          c.setPersonId(nObject.getString("personId"))
          c.setPersonName(nObject.getString("personName"))
          creatorList.add(Until.getDBObjFromJavaBean(c))
        }
        document.put("creator", creatorList)
      }else {
        document.put("creator", "null")
      }

      val peopleCount = row.getAs[String]("peopleCount")
      if(!"null".equals(peopleCount) && peopleCount!=null && peopleCount!="null") {
        val peopleCountArr = JSON.parseArray(peopleCount)
        for (i <- 0 until (peopleCountArr.size())) {
          val nObject = peopleCountArr.getJSONObject(i)
          val c = new PeopleCount
          c.setYear(nObject.getString("year"))
          c.setCount(nObject.getString("count"))

          peopleCountList.add(Until.getDBObjFromJavaBean(c))
        }
        document.put("peopleCount", peopleCountList)
      }else {
        document.put("peopleCount", "null")
      }


      val formerHead = row.getAs[String]("formerHead")
      if(!"null".equals(formerHead) && formerHead!=null && formerHead!="null") {
        val formerHeadArr = JSON.parseArray(formerHead)
        for (i <- 0 until (formerHeadArr.size())) {
          val nObject = formerHeadArr.getJSONObject(i)
          val c = new FormerHead
          c.setPersonId(nObject.getString("personId"))
          c.setPersonName(nObject.getString("personName"))
          c.setStartDate(nObject.getString("startDate"))
          c.setEndDate(nObject.getString("endDate"))
          formerHeadList.add(Until.getDBObjFromJavaBean(c))
        }
        document.put("formerHead", formerHeadList)
      }else {
        document.put("formerHead", "null")
      }


      val head = row.getAs[String]("head")
      if(!"null".equals(head) && head!=null && head!="null") {
        val array = JSON.parseObject(head)
          val author = new FirstAuthor()
          author.setPersonId(array.getString("personId"))
          author.setPersonName(array.getString("personName"))
        document.put("head", Until.getDBObjFromJavaBean(author))
      }else{
        document.put("head", "null")
      }



      val parentOrganization = row.getAs[String]("parentOrganization")
      if(!"null".equals(parentOrganization) && parentOrganization!=null && parentOrganization!="null"){
        val array = JSON.parseObject(parentOrganization)
        val organizationNameAndId = new OrganizationNameAndId()
        organizationNameAndId.setOrganizationId(array.getString("organizationId"))
        organizationNameAndId.setOrganizationName(array.getString("organizationName"))
        document.put("parentOrganization", Until.getDBObjFromJavaBean(organizationNameAndId))
      }else{
        document.put("parentOrganization", "null")
      }

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

