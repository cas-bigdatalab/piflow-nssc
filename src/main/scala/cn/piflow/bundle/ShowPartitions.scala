package cn.piflow.bundle

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.ImageUtil
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}


class ShowPartitions extends ConfigurableStop {
  override val authorEmail: String = "yit"
  override val description: String = "show partitions"
  override val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  override val outportList: List[String] = List(PortEnum.DefaultPort.toString)


  override def setProperties(map: Map[String, Any]): Unit = {

  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] ={
    var descriptor : List[PropertyDescriptor] = List()
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/nssc/wait.png")
  }


  override def getGroup(): List[String] = {

    List(StopGroup.NsscGroup.toString)
  }

  override def initialize(ctx: ProcessContext): Unit = {

  }

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val df = in.read()
    println(df.rdd.getNumPartitions)
    out.write(df)
  }
}