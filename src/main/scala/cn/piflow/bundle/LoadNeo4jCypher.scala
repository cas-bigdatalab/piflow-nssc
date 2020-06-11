package cn.piflow.bundle

import java.util.UUID

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql.SparkSession


class LoadNeo4jCypher extends ConfigurableStop {
  override val authorEmail: String = "yit"
  override val description: String = "add uuid"
  override val inportList: List[String] = List(Port.DefaultPort.toString)
  override val outportList: List[String] = List(Port.DefaultPort.toString)
  var cypher:String = _

  override def setProperties(map: Map[String, Any]): Unit = {
    cypher = MapUtil.get(map,"cypher").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] ={
    var descriptor : List[PropertyDescriptor] = List()
    val inports = new PropertyDescriptor().name("cypher").displayName("cypher").description("cypher").defaultValue("").required(true)
    descriptor = inports :: descriptor
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/nssc/add-uuid.png")
  }

  override def getGroup(): List[String] = {

    List("")
  }

  override def initialize(ctx: ProcessContext): Unit = {

  }

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark = pec.get[SparkSession]()

    /*val spark = SparkSession
      .builder()
      .config("spark.neo4j.bolt.url", "bolt://10.0.88.160:7687")
      .config("spark.neo4j.bolt.user", "neo4j")
      .config("spark.neo4j.bolt.password", "bigdata")
      .getOrCreate()*/

/*

    import org.neo4j.spark._
    val neo = Neo4j(spark.sparkContext)


    println(cypher)
    val frame = neo.cypher(cypher).loadDataFrame
*/


    //out.write(frame)
  }
}