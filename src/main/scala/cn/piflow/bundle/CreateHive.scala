package cn.piflow.bundle

import cn.piflow._
import cn.piflow.conf._
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CreateHive extends ConfigurableStop {

  val authorEmail: String = "xjzhu@cnic.cn"
  val description: String = "Save data to hive"
  val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  val outportList: List[String] = List(PortEnum.NonePort.toString)

  var database:String = _
  var table:String = _

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark = pec.get[SparkSession]()
    val inDF = in.read()



    inDF.createOrReplaceTempView("temp")

    spark.sql("create table " + database + "." + table +  "  as select * from  temp" )

  }

  def initialize(ctx: ProcessContext): Unit = {

  }

  def setProperties(map : Map[String, Any]) = {
    database = MapUtil.get(map,"database").asInstanceOf[String]
    table = MapUtil.get(map,"table").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {
    var descriptor : List[PropertyDescriptor] = List()
    val database=new PropertyDescriptor().name("database").displayName("DataBase").description("The database name").defaultValue("").required(true)
    val table = new PropertyDescriptor().name("table").displayName("Table").description("The table name").defaultValue("").required(true)
    descriptor = database :: descriptor
    descriptor = table :: descriptor
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/hive/PutHiveStreaming.png")
  }

  override def getGroup(): List[String] = {
    List(StopGroup.NsscGroup.toString)
  }


}
