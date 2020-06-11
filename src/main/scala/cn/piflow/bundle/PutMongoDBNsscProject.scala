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


class PutMongoDBNsscProject extends ConfigurableStop{
  override val authorEmail: String = "yit"
  override val description: String = "Put data to mongodb"
  val inportList: List[String] = List(Port.DefaultPort.toString)
  val outportList: List[String] = List(Port.NonePort.toString)

  var ip:String=_
  var port:String=_
  var dataBase:String=_
  var collection:String=_

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
  /*  val spark: SparkSession = pec.get[SparkSession]()
    val df: DataFrame = in.read()
    val list = df.schema.fieldNames.toList
    val list1 = List("organization","charge","chineseKeyword","englishKeyword")
    val list2 = list diff(list1)

    val value: RDD[Document] = df.rdd.map(row => {
      val organizationList = new util.ArrayList[BasicDBObject]()
      val chKeyword = new util.ArrayList[String]()
      val ehKeyword = new util.ArrayList[String]()



      val document = new Document()

      val organization = row.getAs[String]("organization")
      if(!"null".equals(organization) && organization!=null && organization!="null") {
        val array = JSON.parseArray(organization)
        for (i <- 0 until (array.size())) {
          val orgAndId = new OrganizationNameAndId
          val nObject = array.getJSONObject(i)
          orgAndId.setOrganizationId(nObject.getString("organizationId"))
          orgAndId.setOrganizationName(nObject.getString("organizationName"))
          organizationList.add(Until.getDBObjFromJavaBean(orgAndId))
        }
        document.put("organization", organizationList)
      }else{
        document.put("organization", "null")
      }


      val charge = row.getAs[String]("charge")
      if(!"null".equals(charge) && charge!=null && charge!="null"){
        val json1 = JSON.parseObject(charge)
        val f = new FirstAuthor
        f.setPersonId(json1.getString("personId"))
        f.setPersonName(json1.getString("personName"))
        document.put("charge", Until.getDBObjFromJavaBean(f))
      }else{
        document.put("charge", "null")
      }


      val chineseKeyword = row.getAs[String]("chineseKeyword")

      if(!"null".equals(chineseKeyword) && chineseKeyword!=null && chineseKeyword!="null"){
        val reArr = chineseKeyword.split(",")
        reArr.foreach(t=>{
          chKeyword.add(t)
        })
        document.put("chineseKeyword",chKeyword)
      }else{
        document.put("chineseKeyword","null")
      }

      val englishKeyword = row.getAs[String]("englishKeyword")

      if(!"null".equals(englishKeyword) && englishKeyword!=null && englishKeyword!="null"){
        val reArr = englishKeyword.split(",")
        reArr.foreach(t=>{
          ehKeyword.add(t)
        })
        document.put("englishKeyword",ehKeyword)
      }else{
        document.put("englishKeyword","null")
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
