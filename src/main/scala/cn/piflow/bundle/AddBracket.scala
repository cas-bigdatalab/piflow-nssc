package cn.piflow.bundle

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql.SparkSession


class 
AddBracket extends ConfigurableStop {
  override val authorEmail: String = "yit"
  override val description: String = "add bracket"
  override val inportList: List[String] = List(Port.DefaultPort.toString)
  override val outportList: List[String] = List(Port.DefaultPort.toString)
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

    List("")
  }

  override def initialize(ctx: ProcessContext): Unit = {

  }

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    var df = in.read()
    val spark = pec.get[SparkSession]()
    val sqlContext = spark.sqlContext
    sqlContext.udf.register("code",(str:String)=>"["+str+"]")
    val columns: Array[String] = schema.split(",")
    val name = df.schema(0).name
    columns.foreach(t=>{
      df.createOrReplaceTempView("temp")
      val frame = sqlContext.sql("select *,code("+t+") as new from temp")
      df = frame.drop(t).withColumnRenamed("new",t)
    })
    out.write(df)
  }
}