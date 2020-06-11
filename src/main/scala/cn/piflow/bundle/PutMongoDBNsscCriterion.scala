package cn.piflow.bundle

import java.util

import cn.piflow.bundle.entity.{AuthorIdAndNameEamil, OrganizationNameAndId}
import cn.piflow.bundle.util.Until
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import cn.piflow.conf.{ConfigurableStop, Port}
import com.alibaba.fastjson.JSON
import com.mongodb.BasicDBObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document


class PutMongoDBNsscCriterion extends ConfigurableStop{
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
    val list1 = List("author","chargeOrg","publishOrg","centralizedOrg","publishAgency")
    val list2 = list diff(list1)

    val value: RDD[Document] = df.rdd.map(row => {
      val organizationList = new util.ArrayList[BasicDBObject]()
      val authorList = new util.ArrayList[BasicDBObject]()



      val document = new Document()

      val author = row.getAs[String]("author")
      if(!"null".equals(author) && author!=null && author!="null") {
        val array = JSON.parseArray(author)
        for (i <- 0 until (array.size())) {
          val nObject = array.getJSONObject(i)
          val a = new AuthorIdAndNameEamil
          a.setEmail(nObject.getString("email"))
          a.setPersonId(nObject.getString("personId"))
          a.setPersonName(nObject.getString("personName"))
          authorList.add(Until.getDBObjFromJavaBean(a))
        }
        document.put("author", authorList)
      }else{
        document.put("author", "null")
      }



      val organization = row.getAs[String]("publishAgency")
      if(!"null".equals(organization) && organization!=null && organization!="null") {
        val array = JSON.parseArray(organization)
        for (i <- 0 until (array.size())) {
          val orgAndId = new OrganizationNameAndId
          val nObject = array.getJSONObject(i)
          orgAndId.setOrganizationId(nObject.getString("organizationId"))
          orgAndId.setOrganizationName(nObject.getString("organizationName"))
          organizationList.add(Until.getDBObjFromJavaBean(orgAndId))
        }
        document.put("publishAgency", organizationList)
      }else{
        document.put("publishAgency", "null")
      }


      val chargeOrg = row.getAs[String]("chargeOrg")
      if(!"null".equals(chargeOrg) && chargeOrg!=null && chargeOrg!="null"){
        val json1 = JSON.parseObject(chargeOrg)
        val o = new OrganizationNameAndId
        o.setOrganizationId(json1.getString("organizationId"))
        o.setOrganizationName(json1.getString("organizationName"))
        document.put("chargeOrg", Until.getDBObjFromJavaBean(o))
      }else{
        document.put("chargeOrg", "null")
      }



      val centralizedOrg = row.getAs[String]("centralizedOrg")
      if(!"null".equals(centralizedOrg) && centralizedOrg!=null && centralizedOrg!="null"){
        val json1 = JSON.parseObject(centralizedOrg)
        val o = new OrganizationNameAndId
        o.setOrganizationId(json1.getString("organizationId"))
        o.setOrganizationName(json1.getString("organizationName"))
        document.put("centralizedOrg", Until.getDBObjFromJavaBean(o))
      }else{
        document.put("centralizedOrg", "null")
      }


      val publishOrg = row.getAs[String]("publishOrg")
      if(!"null".equals(publishOrg) && publishOrg!=null && publishOrg!="null"){
        val json1 = JSON.parseObject(publishOrg)
        val o = new OrganizationNameAndId
        o.setOrganizationId(json1.getString("organizationId"))
        o.setOrganizationName(json1.getString("organizationName"))
        document.put("publishOrg", Until.getDBObjFromJavaBean(o))
      }else{
        document.put("publishOrg", "null")
      }




      list2.foreach(t=>{
        document.put(t,row.getAs[String](t))
      })

      document
    })
    val str = "mongodb://" + ip + ":" + port + "/" + dataBase + "." + collection
    MongoSpark.save(value,WriteConfig(Map("spark.mongodb.output.uri"->str)))
*/
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
