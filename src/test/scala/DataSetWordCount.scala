import org.apache.spark.sql.{SQLContext}
import com.sncf.fab.ppiv.utils.AppConf._
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by simoh-labdoui on 10/05/2017.
  */
object DataSetWordCount {
  var LOGGER=Logger.getLogger(DataSetWordCount.getClass)

  def main(args: Array[String]) {

    val sparkConf =  new SparkConf()
      .setAppName(PPIV)
      .setMaster(SPARK_MASTER)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)



    import sqlContext.implicits._

    val data = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("src/main/resources/data.csv").as[String];

    val words = data.flatMap(value => value.split("\\s+"))

    val groupedWords = words.groupBy(_.toLowerCase)

    val counts = groupedWords.count()

    counts.show()
    LOGGER.info("is finished")


  }

}
