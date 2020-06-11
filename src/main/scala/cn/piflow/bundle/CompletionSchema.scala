package cn.piflow.bundle

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql.SparkSession


class CompletionSchema extends ConfigurableStop {
  override val authorEmail: String = "yit"
  override val description: String = "completion schema"
  override val inportList: List[String] = List(Port.DefaultPort.toString)
  override val outportList: List[String] = List(Port.DefaultPort.toString)
  var database:String = _
  var table:String = _

  override def setProperties(map: Map[String, Any]): Unit = {
    database = MapUtil.get(map,"database").asInstanceOf[String]
    table = MapUtil.get(map,"table").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] ={
    var descriptor : List[PropertyDescriptor] = List()
    val database = new PropertyDescriptor().name("database").displayName("database").description("The database is you database").defaultValue("").required(true)
    val table = new PropertyDescriptor().name("table").displayName("table").description("The table is you table").defaultValue("").required(true)
    descriptor = database :: descriptor
    descriptor = table :: descriptor
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/nssc/completion-schema.png")
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

    val list = spark.sql("select * from "+database+"."+table).schema.fieldNames.toList

    //var df: DataFrame = spark.sql("select * from nssc.org")
    val list1 = df.schema.fieldNames.toList

    val list2= list diff list1

    import org.apache.spark.sql._

    val name = df.schema(0).name

    val udf_null  = functions.udf((str:Any)=>"null")
    list2.foreach( t=> {
      df = df.withColumn(t,udf_null(functions.col(name)).cast(types.StringType))
    })

    df.createOrReplaceTempView("temp")
    val columns = list.mkString(",")

    val df1 = spark.sql("select "+columns+" from temp")
   // df1.createOrReplaceTempView("temp1")
    //spark.sql("insert into table nssc_v1.organization select * from temp1")


    out.write(df1)
  }
}