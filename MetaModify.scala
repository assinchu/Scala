import java.sql.DriverManager
import org.apache.logging.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.mutable
import play.api.libs.json._
import net.liftweb.json._

object MetaModify {

  val context=new Context();
  //private val tmp_db= "fintmp"
  //private val quoteStr = "`"

  //val usage = """Usage: <jar> --tablename <tablename> --columnnametoadd <column_to_add> --position <position>"""

  implicit val formats = DefaultFormats
  case class Mailserver(url: String, username: String, password: String)

  //type OptionMap = Map[Symbol, String]
  val PARTITIONS=context.partitions

  val log = LogManager.getLogger(context.appname)

  var conf = new SparkConf()
    .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
    .set("spark.executor.heartbeatInterval", "120s")
    .set("spark.sql.orc.filterPushdown", "true")
    .set("spark.network.timeout", "12000s")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "512m")
    .set("spark.streaming.stopGracefullyOnShutdown", "true")
    .set("spark.memory.storageFraction", "0.2")
    .set("spark.memory.fraction","0.8")
    .set("spark.sql.shuffle.partitions", PARTITIONS.toString)
    .set("spark.sql.autoBroadcastJoinThreshold", "104857600")
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.sql.orc.enabled", "true")
    .set("spark.sql.hive.convertMetastoreOrc","true")
    .setAppName(context.appname)

  def main(args: Array[String]): Unit =
  {

    //######################### Parse arguments and validate START ##############################
    /*if (args.length < 3) {
      println(usage)
      log.error("Mandatory parameters not provided")
      println("Mandatory parameters not provided")
      System.exit(1)
    }*/

    //val options = optionToMap(Map(),args.toList)

    /*val runId:String = if(isAllDigits(options.getOrElse('position, "")) == true)
      options.getOrElse('runid, "")
    else{
      log.error("Invalid position provided")
      println("Invalid position provided")
      System.exit(1)
      ""
    }*/
    //val context=new Context();
    //######################### Parse arguments and validate END ##############################

    parseJsonLift("asd")
    parseJsonPlay("asd")


    val spark = SparkSession.builder.config(conf).master("local").enableHiveSupport().getOrCreate()
    println(context.jdbcUrl)

    //val metastoreTableColumnsData = spark.read.jdbc(context.jdbcUrl, context.metastoreTable, context.connectionProperties).where("schema_change = 'True' and locked = '1'").cache()
    val metastoreTableColumnsData = spark.read.jdbc(context.jdbcUrl, context.metastoreTable, context.connectionProperties).cache()
    //val getHiveColumns = spark.sql("""select tablename,primary_keys,columns,partition_columns,locked,schema_change from bdmerge.scope_tables""".stripMargin)
    //val getGpColumns = spark.sql("""select source_tablename,primary_keys,source_columns,partition_columns,locked,schema_change from bdmerge.scope_tables""".stripMargin)

    val metastoreTableData = spark.read.jdbc(context.jdbcUrl, context.metastoreTable, context.connectionProperties).cache()

    import spark.implicits._

    /*val pushdown_query = "show create table actDB.ADMIN_DATA"
    val df1 = spark.read.jdbc(url=context.jdbcUrl, table=pushdown_query, properties=context.connectionProperties)
    println(df1)*/

    val admin_table_schema = spark.read.jdbc(context.jdbcUrl, "ADMIN_DATA", context.connectionProperties)
    println("Schema")
    admin_table_schema.printSchema

    val a =admin_table_schema.select("BRANCH")
    println(a)

    val df: org.apache.spark.sql.DataFrame = spark.read.jdbc(context.jdbcUrl, "ADMIN_DATA", context.connectionProperties)
    println(df)
    df.select()
    df.show()

    //metastoreTableColumnsData.show()

    var changed_tbl_count = metastoreTableColumnsData.count()

    // val metastoreHiveColumnsData_all = metastoreTableColumnsData.select("tablename","columns","partition_columns","primary_keys")
   // var metastoreHiveColumnsData = metastoreTableColumnsData.select("BRANCH_LOCKER_STATUS","columns","precision_columns")
    //println(metastoreHiveColumnsData)
    //metastoreTableColumnsData_gpcols.collect.map(t =>  println(t))

    //metastoreTableColumnsData_gpcols.withColumn("create_stmt", lit("Cre " + aklnsdasklnd + as,mdnasl ""))

    /*def getTableDetails(tablename: String, cols: Array[String],partition_columns:String, primary_keys:String):String = {

      String.format("create table %s.%s " +
        " (" +
        " %s" +
        " ) " +
        "%s" +
        " ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' " +
        " WITH SERDEPROPERTIES ('serialization.format' = '1')" +
        " STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'" +
        " OUTPUTFORMAT  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'" +
        " TBLPROPERTIES ('serialization.null.format' = 'null'," +
        " 'orc.compress' = 'ZLIB' ," +
        " 'orc.stripe.size' = '536870912' ," +
        " 'orc.compress.size' = '268435456' )", tmp_db, tablename, cols.toStream.map((m: FieldSchema) => f.quote(m.getName, quoteStr) + " " + m.getType).collect(Collectors.joining(",")),
        if ((!(partition_columns.isEmpty))) {
        (" PARTITIONED BY( " + partition_columns.stream.map((n: FieldSchema) => f.quote(n.getName, quoteStr) + " " + n.getType).collect(Collectors.joining(",")) + ")")
      }
      else {
        " "
      })
    }

    val query = metastoreTableColumnsData_gpcols.withColumn("create_stmt", lit(getTableDetails($tablename,$columns,$partition_columns,$primary_keys)))
*/

    //def getTableColumns()

    //val getFinanceTableColumns = spark.sql("""describe table finance.xx_bank_branches""")

    //metastoreTableColumnsData.select("tablename","columns","precision_columns").toDF().map(columns => scopeTablesColDiff($"tablename",$"columns",$"precision_columns"))

    //metastoreTableColumnsData.select("tablename","columns","precision_columns").toDF().map(columns => columns.mkString(";")).collect().foreach(value=>scopeTablesColDiff(value))


    def scopeTablesColDiff (value :String):Unit = {

      val column_splitter=value.split(";")
      val tablename = column_splitter(0)
      val columns = column_splitter(1)
      //val precision_columns = column_splitter(2)

      var columnDataMap = new mutable.HashMap[String,String]()
      println(tablename)

      if(!spark.catalog.tableExists(tablename)) {

        println("Table"+tablename+"does not exist")

      }

      else{
        println("Entered if stmt")
        val getFinanceTableColumns = spark.sql("select * from " + tablename + " limit 1")
        //getFinanceTableColumns.dtypes
        //var listColumns = List[String]()
        var listColumns = Array[String]()

        getFinanceTableColumns.dtypes.foreach(column => println(column._1))

        //FINANCE TABLE COLUMNS
        getFinanceTableColumns.dtypes.foreach(column => listColumns = listColumns :+ column._1)

        val financeColumnsWithoutDF = listColumns.toList.toDF()

        val metastoreHiveColumnsWithoutDT = columns.substring(1, columns.length - 1).split('|').toList.toDF().withColumn("DataType", split(col("value"), ":").getItem(1)).withColumn("value", split(col("value"), ":").getItem(0)).toDF()

        val newColsDiff = metastoreHiveColumnsWithoutDT.select("value").except(financeColumnsWithoutDF)

        println("Count of new cols newColsDiff" + newColsDiff.count())

        val newColsDiffDelete = financeColumnsWithoutDF.except(metastoreHiveColumnsWithoutDT.select("value"))

        println("Count of new cols newColsDiffDelete" + newColsDiffDelete.count())



        val newColsDiffFinal = "'" + newColsDiff.collect().map(_.getString(0)).mkString("','") + "'"

        val addColumnsDF = metastoreHiveColumnsWithoutDT.where("value in (" + newColsDiffFinal + ")")

        addColumnsDF.show()

        var alterColumns = ""

        var alterStatement = ""

        //addColumnsDF.map(rows => rows.mkString(",")).collect.foreach(rows => rows.mkString(","))

        val addColumns = addColumnsDF.map(rows => rows.mkString(" ")).collect.mkString(",")
        println(addColumns)

        //      spark.sql("ALTER TABLE" + tablename + "ADD COLUMNS" + newColsDiffFinal)

        fireQuery(addColumns, tablename)
        println("exiting if stmt")


      }

      None
    }


  }
  def connectToMetadb() =
  {
    try
      {
        Class.forName("org.postgresql.Driver")
        Some(
          DriverManager.getConnection(context.jdbcUrl, context.connectionProperties))
      }
    catch
      {
        case classException: ClassNotFoundException=>
        {
          println("Class not found for specified driver - driver failure")
          log.error("Class not found for specified driver - driver failure " + classException.getMessage)
          System.exit(1);
          None
        }
        case e: Throwable => e.printStackTrace()
          println("Failed to connect to metadata, please check the postgres server, username, password and try again")
          log.error("Failed to connect to metadata, please check the postgres server, username, password and try again")
          System.exit(1);
          None
      }
  }

  /*def optionToMap(map : OptionMap, list: List[String]) : OptionMap = {
    list match {
      case Nil => map
      case "--tablename" :: value :: tail =>
        optionToMap(map ++ Map('tablename -> value.toString.trim), tail)
      case "--columnnametoadd" :: value :: tail =>
        optionToMap(map ++ Map('columnnametoadd -> value.toString.trim), tail)
      case "--position" :: value :: tail =>
        optionToMap(map ++ Map('position -> value.toString.trim), tail)
      case option :: tail => println("Unknown option " + option)
        sys.exit(1)
    }
  }*/

  def fireQuery(addColumns :String ,tableName:String) : Unit = {
    println("firing")
    val alterStatement = String.format("alter table %s add columns(%s)",tableName,addColumns)
    println(alterStatement)
    None
  }

  def parseJsonLift(jsnString: String) : Unit = {
    val json = parse(
      """
      {
       "url": "imap.yahoo.com",
       "username": "myusername",
       "password": "mypassword"
      }
      """
    )

    val m = json.extract[Mailserver]
      println(m.url)
      println(m.username)
      println(m.password)
  }

  def parseJsonPlay(jsnString : String): Unit = {
    val json: JsValue = Json.parse("""
{
  "name" : "Watership Down",
  "location" : {
    "lat" : 51.235685,
    "long" : -1.309197
  },
  "residents" : [ {
    "name" : "Fiver",
    "age" : 4,
    "role" : null
  }, {
    "name" : "Bigwig",
    "age" : 6,
    "role" : "Owsla"
  } ]
}
""")
    println(json)
  }
}


