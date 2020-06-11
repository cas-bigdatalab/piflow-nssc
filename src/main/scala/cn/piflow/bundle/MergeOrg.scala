package cn.piflow.bundle

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql.SparkSession


class MergeOrg extends ConfigurableStop {
  override val authorEmail: String = "yit"
  override val description: String = "add bracket"
  override val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  override val outportList: List[String] = List(PortEnum.DefaultPort.toString)
  var schema:String = _


  override def setProperties(map: Map[String, Any]): Unit = {
    schema = MapUtil.get(map,"schema").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] ={
    var descriptor : List[PropertyDescriptor] = List()
    val schema = new PropertyDescriptor().name("schema").displayName("schema").description("The schema is you want to add bracket schema's name").defaultValue("").required(true)
    descriptor = schema :: descriptor
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/nssc/add-bracket.png")
  }


  override def getGroup(): List[String] = {

    List(StopGroup.NsscGroup.toString)
  }

  override def initialize(ctx: ProcessContext): Unit = {

  }

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    var df = in.read()
    val spark = pec.get[SparkSession]()
    val personDf = spark.sql("select *  from bacco.org")
    val resDf = df.union(personDf)

    out.write(resDf)
  }
}