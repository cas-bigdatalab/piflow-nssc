package cn.piflow.bundle

import java.text.SimpleDateFormat
import java.util.Date

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql.SparkSession


class AddDateStop extends ConfigurableStop {
  override val authorEmail: String = "yit"
  override val description: String = "add uuid"
  override val inportList: List[String] = List(Port.DefaultPort.toString)
  override val outportList: List[String] = List(Port.DefaultPort.toString)
  var column:String = _

  override def setProperties(map: Map[String, Any]): Unit = {
    column = MapUtil.get(map,"column").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] ={
    var descriptor : List[PropertyDescriptor] = List()
    val inports = new PropertyDescriptor().name("column").displayName("column").description("The column is you want to add uuid column's name").defaultValue("").required(true)
    descriptor = inports :: descriptor
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/nssc/add-date.png")
  }

  override def getGroup(): List[String] = {

    List("")
  }

  override def initialize(ctx: ProcessContext): Unit = {

  }

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    var df = in.read()

    val spark = pec.get[SparkSession]()
    val sqlContext = spark.sqlContext
    val name  = df.schema(0).name
    sqlContext.udf.register("code",(str:String)=>{
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      format.format(new Date())
    })
    val columns = column.split(",")
    columns.foreach(t=>{
      df.createOrReplaceTempView("temp")
      df = sqlContext.sql("select *,code("+name+") as "+t +" from temp")
    })
    out.write(df)
  }
}