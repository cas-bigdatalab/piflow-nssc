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


class PutMongoDBNsscPatent extends ConfigurableStop{
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
    val list1 = List("authors","agentOrg")
    val list2 = list diff(list1)

    val value: RDD[Document] = df.rdd.map(row => {
    //  val correspondingAuthorList = new util.ArrayList[BasicDBObject]()
      val auhtorList = new util.ArrayList[BasicDBObject]()
     // val firstAuthorList = new util.ArrayList[BasicDBObject]()
      val organizationList = new util.ArrayList[BasicDBObject]()
      val reList = new util.ArrayList[String]()

      val document = new Document()

      val agentOrg = row.getAs[String]("agentOrg")
      if(!"null".equals(agentOrg) && agentOrg!=null && agentOrg!="null"){
      val json = JSON.parseObject(agentOrg)
      val o=new OrganizationNameAndId
        o.setOrganizationId(json.getString("organizationId"))
        o.setOrganizationName(json.getString("organizationName"))
      document.put("agentOrg", Until.getDBObjFromJavaBean(o))
      }else{
        document.put("agentOrg","null")
      }



      val authors = row.getAs[String]("authors")
      if(!"null".equals(authors) && authors!=null && authors!="null"){

      val authorArr = JSON.parseArray(authors)
      for (i <- 0 until (authorArr.size())) {
        val nObject = authorArr.getJSONObject(i)
        val a = new AuthorIdAndNameEamil
        a.setEmail(nObject.getString("email"))
        a.setPersonId(nObject.getString("personId"))
        a.setPersonName(nObject.getString("personName"))
        auhtorList.add(Until.getDBObjFromJavaBean(a))
       }
        document.put("authors", auhtorList)
      }else{
        document.put("authors", "null")
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
