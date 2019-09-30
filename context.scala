class Context {
  import java.io.{FileInputStream, FileNotFoundException}
  import java.util.Properties
  import org.apache.log4j.LogManager

  val appProperty = new Properties()
  val appname = s"meta_add"
  val log = LogManager.getLogger(appname)
  val partitions=10
  try{
    appProperty.load(new FileInputStream("C:\\Users\\sharmaa\\IdeaProjects\\ScalaDemo\\src\\main\\scala\\meta.properties"))
    println("meta.properties file found")
  }
  catch{
    case fileNotFound: FileNotFoundException => {
      println("meta.properties file not found")
      log.error("meta.properties file not found at specified path, please pass the file path to --files" + fileNotFound.getMessage)
    }
    case e: Throwable => {
      e.printStackTrace()
      log.error("Error opening the property file")
    }
  }
  val connectionProperties = new Properties()
  connectionProperties.put("user", appProperty.getProperty("metastoreUser"))
  connectionProperties.put("password", appProperty.getProperty("metastorePassword"))
  connectionProperties.put("driver", appProperty.getProperty("metastoreDriver"))
  val jdbcHostname = appProperty.getProperty("jdbcHostname")
  val jdbcPort = appProperty.getProperty("jdbcPort")
  val jdbcDatabase = appProperty.getProperty("jdbcDatabase")
  val metastoreSchema = appProperty.getProperty("metastoreschema") + "."
  val metastoreTable = metastoreSchema + appProperty.getProperty("metastoretable")
  //val metastoretablename = metastoreSchema + appProperty.getProperty("metastoretablename")
  val metastoregpcolumns = metastoreTable + appProperty.getProperty("metastoregpcolumns")
  val metastorehivecolumns = metastoreTable + appProperty.getProperty("metastorehivecolumns")
  val metastorehivepartition = metastoreTable + appProperty.getProperty("metastorehivepartition")
  val jdbcUrl = s"jdbc:" + appProperty.getProperty("metastoreType") + "://" + jdbcHostname + ":" + jdbcPort + "/" + jdbcDatabase
}
