package cn.piflow.bundle

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql.SparkSession


class AddColumn extends ConfigurableStop {
  override val authorEmail: String = "yit"
  override val description: String = "add column"
  override val inportList: List[String] = List(Port.DefaultPort.toString)
  override val outportList: List[String] = List(Port.DefaultPort.toString)
  var schema:String = _


  override def setProperties(map: Map[String, Any]): Unit = {
    schema = MapUtil.get(map,"schema").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] ={
    var descriptor : List[PropertyDescriptor] = List()
    val schema = new PropertyDescriptor().name("schema").displayName("schema").description("schema name->value,The name is you want to add  column's name,the value is yout want to add column's value").defaultValue("").required(true)
    descriptor = schema :: descriptor
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/nssc/add-column.png")
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
    val columns: Array[String] = schema.split(",")
    columns.foreach(t=>{
      val columnAndValue: Array[String] = t.split("->")
      val name  = df.schema(0).name
      sqlContext.udf.register("code",(str:String)=>columnAndValue(1))
      df.createOrReplaceTempView("temp")
      df = sqlContext.sql("select *,code("+name+") as "+columnAndValue(0) +" from temp")
    })

    out.write(df)
  }
}