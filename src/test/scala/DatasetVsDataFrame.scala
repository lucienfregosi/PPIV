import com.sncf.fab.myfirstproject.utils.AppConf._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Created by simoh-labdoui on 10/05/2017.
  */
object DatasetVsDataFrame {
  var LOGGER=Logger.getLogger(DatasetVsDataFrame.getClass)

  case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master(SPARK_MASTER)
      .appName(SAMPLE)
      .getOrCreate()

    import sparkSession.implicits._


    //read data from text file

    val df = sparkSession.read.option("header","true").option("inferSchema","true").csv("src/main/resources/sales.csv")
    val ds = sparkSession.read.option("header","true").option("inferSchema","true").csv("src/main/resources/sales.csv").as[Sales]
    val selectedDF = df.select("itemId")
    LOGGER.info("count: " +selectedDF.count())

    val selectedDS = ds.map(_.itemId)
    LOGGER.info("dataframe: ")

    LOGGER.info(selectedDF.queryExecution.optimizedPlan.numberedTreeString)
    LOGGER.info("dataset: ")

    LOGGER.info(selectedDS.queryExecution.optimizedPlan.numberedTreeString)









  }

}
