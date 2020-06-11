package cn.piflow.bundle

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}


class SetPartitions extends ConfigurableStop {
  override val authorEmail: String = "yit"
  override val description: String = "set partitions"
  override val inportList: List[String] = List(Port.DefaultPort.toString)
  override val outportList: List[String] = List(Port.DefaultPort.toString)

  var number:String=_

  override def setProperties(map: Map[String, Any]): Unit = {
    number = MapUtil.get(map,"number").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] ={
    var descriptor : List[PropertyDescriptor] = List()

    val number=new PropertyDescriptor().name("number").displayName("number").description("number is your spark partitions number").defaultValue("").required(true)
    descriptor = number :: descriptor

    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/nssc/fenqu.png")
  }


  override def getGroup(): List[String] = {

    List("")
  }

  override def initialize(ctx: ProcessContext): Unit = {

  }

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {

    val df = in.read()
    if(number.equals("")){
      out.write(df)
    }else{
      val df1 = df.repartition(number.toInt)
      out.write(df1)
    }

  }
}