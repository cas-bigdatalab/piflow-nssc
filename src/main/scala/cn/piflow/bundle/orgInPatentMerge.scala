package cn.piflow.bundle

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql.SparkSession


class orgInPatentMerge extends ConfigurableStop {
  override val authorEmail: String = "yit"
  override val description: String = "add bracket"
  override val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  override val outportList: List[String] = List(PortEnum.DefaultPort.toString)
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

    List(StopGroup.NsscGroup.toString)
  }

  override def initialize(ctx: ProcessContext): Unit = {

  }

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark = pec.get[SparkSession]()
    var frame = spark.sql("select *  from nssc_v15.organization_middle_1")

    val patentDf = spark.sql("select *  from nssc_v15.patent_org_not_in_paper")
    patentDf.createOrReplaceTempView("temp")
    patentDf.show()
    spark.sql("select  *   from temp")
        .createOrReplaceTempView("temp1")
    /*spark.sql("select  *   from temp where organization_id not in (select organization_id from nssc_v15.paper_org)")
      .show()*/
    spark.sql("select organization_id,count(1) as patent_count from temp1 group by organization_id")
        .createOrReplaceTempView("temp2")
    val df = spark.sql("select  organization_id,0.1*patent_count  as patent_count,0.1*patent_count *15 as patent_in  from temp2")


    spark.sql("select  *   from nssc_v15.patent_org_in_paper")
        .createOrReplaceTempView("temp3")
    spark.sql("select organization_id,count(1) as patent_count from temp3 group by organization_id")
        .createOrReplaceTempView("temp4")
    val df1 = spark.sql("select  organization_id,patent_count,patent_count *15 as patent_in  from temp4")

    df.show()
    var dfRes=df.union(df1)
    //frame = frame.withColumnRenamed("organization_id","organization_id_1")
    dfRes = dfRes.withColumnRenamed("organization_id","organization_id_2")

    frame.join(dfRes,frame("organization_id")===dfRes("organization_id_2"),"left_outer")
        .createOrReplaceTempView("res")


    spark.sql("create table nssc_v15.organization_middle_2 as select *   from res")


//    out.write(dfRes)
  }
}