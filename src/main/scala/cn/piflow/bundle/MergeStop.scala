package cn.piflow.bundle

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql.{DataFrame, SparkSession}


class  MergeStop extends ConfigurableStop {
  override val authorEmail: String = "yit"
  override val description: String = "concat"
  override val inportList: List[String] = List(Port.DefaultPort.toString)
  override val outportList: List[String] = List(Port.DefaultPort.toString)
  var schema:String = _
  var column:String = _
  var groupBySchema:String = _

  override def setProperties(map: Map[String, Any]): Unit = {
    schema = MapUtil.get(map,"schema").asInstanceOf[String]
    column = MapUtil.get(map,"column").asInstanceOf[String]
    groupBySchema = MapUtil.get(map,"groupBySchema").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] ={
    var descriptor : List[PropertyDescriptor] = List()
    val schema = new PropertyDescriptor().name("schema").displayName("schema").description("The schema is you want to merge  schema's name").defaultValue("").required(true)
    descriptor = schema :: descriptor

    val column = new PropertyDescriptor().name("column").displayName("column").description("The column is you want to merged  column's name").defaultValue("").required(true)
    descriptor = column :: descriptor

    val groupBySchema = new PropertyDescriptor().name("groupBySchema").displayName("groupBySchema").description("The groupBySchema is you want to merge  by schema's name").defaultValue("").required(true)
    descriptor = groupBySchema :: descriptor
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/nssc/concat.png")
  }

  override def getGroup(): List[String] = {
    List("")
  }

  override def initialize(ctx: ProcessContext): Unit = {

  }

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    var df = in.read()
    val spark = pec.get[SparkSession]()
    df.createOrReplaceTempView("temp")

    df = spark.sql("select "+groupBySchema+",concat_ws(',',collect_list("+schema+")) as "+column+" from temp group by "+groupBySchema).cache()

    out.write(df)
  }
}