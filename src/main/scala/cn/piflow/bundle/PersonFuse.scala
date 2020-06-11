package cn.piflow.bundle

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.ImageUtil
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


class PersonFuse extends ConfigurableStop with Serializable {
  override val authorEmail: String = "yit"
  override val description: String = "fuse"
  override val inportList: List[String] = List(Port.DefaultPort.toString)
  override val outportList: List[String] = List(Port.DefaultPort.toString)


  override def setProperties(map: Map[String, Any]): Unit = {

  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] ={
    var descriptor : List[PropertyDescriptor] = List()
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/nssc/fuse.png")
  }


  override def getGroup(): List[String] = {

    List("")
  }

  override def initialize(ctx: ProcessContext): Unit = {

  }

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val df = in.read()

    val spark = pec.get[SparkSession]()

    def encoder(columns: Seq[String]): Encoder[Row] = RowEncoder(
      StructType(columns.map(StructField(_, StringType, nullable = false))))

    val outputCols = Seq("person_name", "person_id")

    val outDf: DataFrame = df.groupByKey(_.get(0))(Encoders.kryo[Any])
      .flatMapGroups((key, rowsForEach) => {
        val list1 = scala.collection.mutable.ListBuffer[Row]()
        var data: ArrayBuffer[Set[String]] = ArrayBuffer()
        for (tag <- rowsForEach) {

          var set: Set[String] = Set()
          var aa = tag.get(1).toString()

          var tagArr: Array[String] = aa.split(",")
          for (j <- 0 until tagArr.length) {
            set += tagArr(j)

          }
          data.append(set)
        }

        val buffer: ListBuffer[Set[String]] = fusion(data)
        var str_3 = ""
        for (x <- buffer.toList) {
          var str = x.toString()
          var str_2 = str.substring(str.indexOf(",") - 32, str.indexOf(")")).replace(" ", "")
          str_3 += str_2 + "|"
        }
        var str_4 = str_3.substring(0, str_3.length - 1)

        list1.append(Row(key, str_4))
        list1
      })(encoder(outputCols)).toDF

    val uuid = "regexp_replace(reflect(\"java.util.UUID\",\"randomUUID\"),'-','')"
    val time = "reflect(\"java.lang.System\",\"currentTimeMillis\")"

    outDf.createOrReplaceTempView("aa")
    spark.sql(s"""
                 |select
                 |person_name,
                 |if(true,${uuid},null) new_person_id,
                 |explode(split(translate(person_id,"|",";"),";")) as person_id
                 |from aa
    """.stripMargin).createOrReplaceTempView("bb")

    val frame = spark.sql(
      """
           |select
           |person_name,
           |new_person_id,
           |explode(split(person_id,",")) as person_id
           |from bb
               """.stripMargin)

    out.write(frame)
  }


  def fusion(data: ArrayBuffer[Set[String]]): ListBuffer[Set[String]] = {

    // 初始化一个跟groupWithSrc一样大小的数组, 全为-1
    val array = List.fill[Int](data.length)(-1).toArray
    val map = mutable.HashMap[String, ArrayBuffer[Int]]()
    for (i <- data.indices) {
      data(i).foreach(f = s => {
        if (map.contains(s)) {
          // 如果已经存在
          map(s).append(i)
          // 在原来的基础上增加现在的索引
        } else {
          map.put(s, ArrayBuffer(i))
        }
      })
    }
    val values = map.values.toList.sortWith((a, b) => a.mkString("") < b.mkString(""))
    // 排序
    //println(values)
    values.foreach(v => {
      val indexs: ArrayBuffer[Int] = v.sortWith((a, b) => a < b)
      indexs.foreach(i => {
        val arrayI = array(i)
        if (indexs.size == 1) {
          if (arrayI == -1) {
            // 如果index只有一个而且位置为-1则直接插入
            array(i) = i
          }
        } else {
          // 如果index不止一个
          val indValues = indexs.map(array(_)).filter(_ != -1)
          val min = if (indValues.isEmpty) i else indValues.min
          // 找到所有索引位置最小的值, 如果没有就当前值
          if (arrayI == -1) {
            //如果位置为-1 则直接插入
            array(i) = min
          } else {
            // 如果位置已经有值, 则比对原值是否比现在值小, 插入小的
            array(i) = if (arrayI < min) arrayI else {
              values.foreach(arr => {
                // 如果有改动, 则所有包含这个索引的都需要改动
                if (arr.contains(i)) {
                  arr.foreach(ei => array(ei) = min)
                }
              })
              min
            }
          }
        }
      })
    })
    val tmap = new mutable.HashMap[Int, ListBuffer[Int]]()
    for (i <- array.indices) {
      val e = array(i)
      if (tmap.contains(e)) {
        tmap(e).append(i)
      } else {
        tmap.put(e, ListBuffer(i))
      }
    }
    var newBuffer = new ListBuffer[Set[String]]

    tmap.values.foreach(v => {
      var set: Set[String] = Set()
      v.foreach(set ++= data(_))
      newBuffer.append(set)
    })
    newBuffer
  }
}