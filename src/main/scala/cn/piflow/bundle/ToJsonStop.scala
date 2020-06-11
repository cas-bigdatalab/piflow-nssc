package cn.piflow.bundle

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql.functions._


class ToJsonStop extends ConfigurableStop {
  override val authorEmail: String = "yit"
  override val description: String = "to json"
  override val inportList: List[String] = List(Port.DefaultPort.toString)
  override val outportList: List[String] = List(Port.DefaultPort.toString)

  var column:String = _
  var schema:String = _
  override def setProperties(map: Map[String, Any]): Unit = {
    column = MapUtil.get(map,"column").asInstanceOf[String]
    schema = MapUtil.get(map,"schema").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] ={
    var descriptor : List[PropertyDescriptor] = List()
    val column = new PropertyDescriptor().name("column").displayName("column").description("The column is you want to add json column's name").defaultValue("").required(true)
    descriptor = column :: descriptor

    val schema = new PropertyDescriptor().name("schema").displayName("schema").description("The schema is you want to add json").defaultValue("").required(true)
    descriptor = schema :: descriptor
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/nssc/to-json.png")
  }

  override def getGroup(): List[String] = {

    List("")
  }

  override def initialize(ctx: ProcessContext): Unit = {

  }

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    var df = in.read()

    val schemas = schema.split(",")
    if(schemas.length==1){
        df = df.withColumn(column,to_json(struct(schemas(0))))
    }
    else if (schemas.length==2){
        df = df.withColumn(column,to_json(struct(schemas(0),schemas(1))))
    }
    else if (schemas.length==3){
        df = df.withColumn(column,to_json(struct(schemas(0),schemas(1),schemas(2))))
    }
    else if (schemas.length==4){
      df = df.withColumn(column,to_json(struct(schemas(0),schemas(1),schemas(2),schemas(3))))
    }
    else if (schemas.length==5){
      df = df.withColumn(column,to_json(struct(schemas(0),schemas(1),schemas(2),schemas(3),schemas(4))))
    }
    else if (schemas.length==6){
      df = df.withColumn(column,to_json(struct(schemas(0),schemas(1),schemas(2),schemas(3),schemas(4),schemas(5))))
    }else if (schemas.length==7){
      df = df.withColumn(column,to_json(struct(schemas(0),schemas(1),schemas(2),schemas(3),schemas(4),schemas(5),schemas(6))))
    }else if (schemas.length==8){
      df = df.withColumn(column,to_json(struct(schemas(0),schemas(1),schemas(2),schemas(3),schemas(4),schemas(5),schemas(6),schemas(7))))
    }

    out.write(df)
  }
}