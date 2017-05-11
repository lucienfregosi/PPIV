import org.apache.spark.sql.SparkSession
import com.sncf.fab.myfirstproject.utils.AppConf._
import org.apache.log4j.Logger

/**
  * Created by simoh-labdoui on 10/05/2017.
  */
object DataSetWordCount {
  var LOGGER=Logger.getLogger(DataSetWordCount.getClass)

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master(SPARK_MASTER)
      .appName(SAMPLE)
      .getOrCreate()

    import sparkSession.implicits._
    val data = sparkSession.read.text("src/main/resources/data.txt").as[String]

    val words = data.flatMap(value => value.split("\\s+"))

    val groupedWords = words.groupByKey(_.toLowerCase)

    val counts = groupedWords.count()

    counts.show()
    LOGGER.info("is finished")


  }

}
