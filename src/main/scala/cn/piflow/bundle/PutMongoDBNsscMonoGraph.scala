package cn.piflow.bundle

import java.util

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


class PutMongoDBNsscMonoGraph extends ConfigurableStop with Serializable {
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
    val list1 = List("organizationAuthor","author")
    val list2 = list diff(list1)

    val value: RDD[Document] = df.rdd.map(row => {
    //  val correspondingAuthorList = new util.ArrayList[BasicDBObject]()
      val auhtorList = new util.ArrayList[BasicDBObject]()
      val orgList = new util.ArrayList[BasicDBObject]()
     // val firstAuthorList = new util.ArrayList[BasicDBObject]()

      val document = new Document()

      val organizationAuthor = row.getAs[String]("organizationAuthor")
      if(!"null".equals(organizationAuthor) && organizationAuthor!=null && organizationAuthor!="null") {
        val organizationAuthorArr = JSON.parseArray(organizationAuthor)
        for (i <- 0 until (organizationAuthorArr.size())) {
          val nObject = organizationAuthorArr.getJSONObject(i)
          val o = new OrganizationNameAndId
          o.setOrganizationId(nObject.getString("organizationId"))
          o.setOrganizationName(nObject.getString("organizationName"))
          orgList.add(Until.getDBObjFromJavaBean(o))
        }
        document.put("organizationAuthor", orgList)
      }else{
        document.put("organizationAuthor","null")
      }



      val author = row.getAs[String]("author")
      if(!"null".equals(author) && author!=null && author!="null"){

      val authorArr = JSON.parseArray(author)
      for (i <- 0 until (authorArr.size())) {
        val nObject = authorArr.getJSONObject(i)
        val a = new AuthorIdAndNameEamil
        a.setEmail(nObject.getString("email"))
        a.setPersonId(nObject.getString("personId"))
        a.setPersonName(nObject.getString("personName"))
        auhtorList.add(Until.getDBObjFromJavaBean(a))
       }
        document.put("author", auhtorList)
      }else{
        document.put("author", "null")
      }

      list2.foreach(t=>{
        document.put(t,row.getAs[String](t))
      })

      document
    })
    import com.mongodb.spark._
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
