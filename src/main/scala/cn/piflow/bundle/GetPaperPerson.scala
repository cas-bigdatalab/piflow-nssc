package cn.piflow.bundle

import cn.piflow._
import cn.piflow.conf._
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class GetPaperPerson extends ConfigurableStop with Serializable {

  val authorEmail: String = "yit"
  val description: String = "get paper person"
  val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  val outportList: List[String] = List(PortEnum.DefaultPort.toString)

  var column:String = _

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark = pec.get[SparkSession]()
    val inDF = in.read()
    var organizationName:String= ""
    var paperId:String= ""
    var year:Integer=null
    var arr = Array[String]()

    val value: RDD[(String,String,Integer,Array[String])] = inDF.rdd.map(t => {
      val personName = t.getAs[String]("personName")
      if(personName.contains("|")){
        arr = personName.split("\\|")
        organizationName = t.getAs[String]("organizationName")
        paperId = t.getAs[String](column)
        year = t.getAs[Integer]("year")
      }else if (personName.contains("，")){
        arr = personName.split("，")
        organizationName = t.getAs[String]("organizationName")
        paperId = t.getAs[String](column)
        year = t.getAs[Integer]("year")
      }else if(personName.contains("; ")){
        arr = personName.split("; ")
        organizationName = t.getAs[String]("organizationName")
        paperId = t.getAs[String](column)
        year = t.getAs[Integer]("year")
      }else if(personName.contains(", ")){
        arr = personName.split(", ")
        organizationName = t.getAs[String]("organizationName")
        paperId = t.getAs[String](column)
        year = t.getAs[Integer]("year")
      } else{
        arr = personName.split("\\+")
        organizationName = t.getAs[String]("organizationName")
        paperId = t.getAs[String](column)
        year = t.getAs[Integer]("year")
      }


      (paperId,organizationName,year,arr)
    })

    val unit: RDD[Array[(String,String,String,String,Integer, Int)]] = value.map(t => {
      var i=0
      val tuples: Array[(String,String,String,String,Integer,Int)] = t._4.map(name => {
          i=i+1
          (t._1,t._2,name, name+"|"+t._2,t._3,i)
      })
      tuples
    })
    val unit1: RDD[(String,String,String,String, Integer,Int)] = unit.flatMap(t => {
      t
    })
    import spark.implicits._
    val df: DataFrame = unit1.toDF(column,"organizationName","personName","person_and_org","year","id")
    out.write(df)

  }

  def initialize(ctx: ProcessContext): Unit = {

  }

  def setProperties(map : Map[String, Any]) = {
    column = MapUtil.get(map,"column").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {
    var descriptor : List[PropertyDescriptor] = List()
    val inports = new PropertyDescriptor().name("column").displayName("column").description("The column is you want to add uuid column's name").defaultValue("").required(true)
    descriptor = inports :: descriptor
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/nssc/wait.png")
  }

  override def getGroup(): List[String] = {
    List(StopGroup.NsscGroup.toString)
  }


}
