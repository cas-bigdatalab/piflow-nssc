package cn.piflow.bundle

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}

class DropColumn extends ConfigurableStop{
  override val authorEmail: String = "yit"
  override val description: String = "drop column"
  override val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  override val outportList: List[String] = List(PortEnum.DefaultPort.toString)

  var column:String=_
  override def setProperties(map: Map[String, Any]): Unit = {
    column = MapUtil.get(map,"column").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {
    var descriptor : List[PropertyDescriptor] = List()
    val inports = new PropertyDescriptor().name("column").displayName("column").description("The column is you want to drop column").defaultValue("").required(true)
    descriptor = inports :: descriptor
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/nssc/drop-column.png")
  }

  override def getGroup(): List[String] = {
    List(StopGroup.NsscGroup.toString)
  }

  override def initialize(ctx: ProcessContext): Unit = {

  }

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val columns: Array[String] = column.split(",")
    var df = in.read()
    columns.foreach(t=>{
      df = df.drop(t)
    })
    out.write(df)
  }
}
