package cn.piflow.bundle

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.ImageUtil
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}


class ToJson extends ConfigurableStop {
  override val authorEmail: String = "yit"
  override val description: String = "to json"
  override val inportList: List[String] = List(Port.DefaultPort.toString)
  override val outportList: List[String] = List(Port.DefaultPort.toString)
  //var schema:String = _
  //var column:String = _

  override def setProperties(map: Map[String, Any]): Unit = {
    //schema = MapUtil.get(map,"schema").asInstanceOf[String]
   // column = MapUtil.get(map,"column").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] ={
    var descriptor : List[PropertyDescriptor] = List()
    val inports = new PropertyDescriptor().name("column").displayName("column").description("The column is you want to add uuid column's name").defaultValue("").required(true)
    descriptor = inports :: descriptor
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
    val spark = pec.get[SparkSession]()
    //val schemaArr = schema.split(",")
    //schemaArr.foreach(t=>{})
    //df=df.withColumn("abcd",to_json(st))
    import org.apache.spark.sql.functions._

    val df1 = df.withColumn("current_organization",to_json(struct("organizationId","organizationName","department","type"))).cache()


   // val df2 = df1.withColumn("work_experience",to_json(struct("title","personId","startDate","endDate","isNow","organizationId","organization","nationality","province","city","address"))).cache()
    val df2 = df1.withColumn("work_experience",to_json(struct("title","startDate","endDate","organizationId","organizationName","department"))).cache()

    val df3 = df2.withColumn("current_title",to_json(struct("title","organizationId","organizationName","department","date"))).cache()

    val df4 = df3.withColumn("new_degree",to_json(struct("degree","major","organizationId","organizationName","country","countryId","startDate","endDate"))).cache()

    val df5 = df4.drop("degree").withColumnRenamed("new_degree","degree").cache()

    val df6 = df5.drop("title").withColumnRenamed("现任职称","title").cache()

    val df7 = df6.withColumn("current_position",to_json(struct("title","organizationId","organizationName","department","date"))).cache()
    df6.select("personId").show()
    df7.select("personId").show()
    println(df7.schema.fieldNames.toBuffer)
    val df8 = df7.drop("title").cache()
      //.withColumnRenamed(" 学术任职（主席、委员等）","title1")



    val df9 = df8.withColumn("title",explode(split(df("title1"),"，"))).cache()

    val df10 = df9.withColumn("academic_title",to_json(struct("type","organizationId","organizationName","startDate","endDate","title"))).cache()
    println(df.count())

    //df.select("title","title1").show()
    df10.select("personId","academic_title").createOrReplaceTempView("temp")

    val df11 = spark.sql("select personId,concat_ws(',',collect_list(academic_title)) as new_academic_title from temp group by personId").cache()

    println(df11.count())

    df11.show()

    //df = df.join(df1,df("personId"),"left")
    //df = df.withColumn("new_academic_title",concat_ws(",",collect_set("academic_title")))


    val df12 = df8.join(df11,Seq("personId"),"left").cache()

    val df13 = df12.withColumnRenamed("new_academic_title","academic_title").cache()


    val df14 = df13.withColumnRenamed("电话","number")
      .withColumn("contact1",to_json(struct("contactType","number")))
      .cache()

    df14.show()
    val df15 = df14.drop("contactType").drop("number").withColumnRenamed("contactType1","contactType")
        .withColumnRenamed("手机","number")
        .withColumn("contact2",to_json(struct("contactType","number")))
        .cache()
    /*df.select("personId","title")


    explode(split(df("title"),","))*/
    df15.show()
    val df16: DataFrame = df15.drop("contactType").drop("number").withColumnRenamed("contactType2", "contactType")
      .withColumnRenamed("chuanzhen", "number")
      .withColumn("contact3", to_json(struct("contactType", "number")))
      .cache()

    val df17 = df16.withColumn("contact",concat_ws(",",col("contact1"),col("contact2"),col("contact3"))).cache()



    var df18 = df17.withColumnRenamed("人才计划","name")



    val udf_null  = functions.udf((str:Any)=>"")

    if(df18.select("name").equals("")){
       df18 = df18.withColumn("outstanding",udf_null(df18("name")))
        .cache()
    }else{
       df18 = df18.withColumn("outstanding", to_json(struct("name", "date")))
        .cache()
    }



    println(df18.schema.fieldNames.toBuffer)
    df18.show(5)
    out.write(df18)
  }
}