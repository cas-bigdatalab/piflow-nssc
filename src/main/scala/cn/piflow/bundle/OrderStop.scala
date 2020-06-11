package cn.piflow.bundle

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql.SparkSession


class OrderStop extends ConfigurableStop {
  override val authorEmail: String = "yit"
  override val description: String = "order"
  override val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  override val outportList: List[String] = List(PortEnum.DefaultPort.toString)
  var schema:String = _
  var orderRule:String = _


  override def setProperties(map: Map[String, Any]): Unit = {
    schema = MapUtil.get(map,"schema").asInstanceOf[String]
    orderRule = MapUtil.get(map,"orderRule").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] ={
    var descriptor : List[PropertyDescriptor] = List()
    val inports = new PropertyDescriptor().name("schema").displayName("column").description("The column is you want to order column's name").defaultValue("").required(true)
    descriptor = inports :: descriptor


    val orderRule = new PropertyDescriptor().name("orderRule").displayName("orderRule").description("The orderRule is you want to order type you can choose acs and desc default asc").defaultValue("").required(true)
    descriptor = orderRule :: descriptor
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/nssc/order.png")
  }

  override def getGroup(): List[String] = {

    List(StopGroup.NsscGroup.toString)
  }

  override def initialize(ctx: ProcessContext): Unit = {

  }

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    var df = in.read()
    import org.apache.spark.sql.functions._

    orderRule match {
      case "desc" => df = df.orderBy(desc(schema))
      case "asc" => df = df.orderBy(asc(schema))
      case "" =>  df = df.orderBy(asc(schema))
    }

    out.write(df)
  }
}