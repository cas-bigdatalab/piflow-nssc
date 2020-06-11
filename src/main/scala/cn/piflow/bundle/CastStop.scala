package cn.piflow.bundle

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql.{KeyValueGroupedDataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer


class CastStop extends ConfigurableStop {
  override val authorEmail: String = "yit"
  override val description: String = "cast"
  override val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  override val outportList: List[String] = List(PortEnum.DefaultPort.toString)
  var schema:String = _
  var mold:String = _

  override def setProperties(map: Map[String, Any]): Unit = {
    schema = MapUtil.get(map,"schema").asInstanceOf[String]
    mold = MapUtil.get(map,"mold").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] ={
    var descriptor : List[PropertyDescriptor] = List()
    val schema = new PropertyDescriptor().name("schema").displayName("schema").description("The schema is you want to cast  schema's name").defaultValue("").required(true)
    descriptor = schema :: descriptor

    val mold = new PropertyDescriptor().name("mold").displayName("mold").description("The mold is you want to cast type").defaultValue("").required(true)
    descriptor = mold :: descriptor
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/nssc/cast.png")
  }

  override def getGroup(): List[String] = {

    List(StopGroup.NsscGroup.toString)
  }

  override def initialize(ctx: ProcessContext): Unit = {

  }

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    var df = in.read()
    val spark = pec.get[SparkSession]()
    val list = df.schema.fieldNames.toList
    val sb = new ArrayBuffer[String]()
    list.foreach(t=>{
      if(t.equals(schema)){
        sb.append("cast ("+t+" as "+mold+") as "+t)
      }else{
        sb.append(t)
      }
    })

    val listStr: String = sb.toList.mkString(",")

    df.createOrReplaceTempView("temp")

    df = spark.sql("select "+listStr+" from temp").cache()

    df.printSchema()

    out.write(df)
  }
}