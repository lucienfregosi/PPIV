import com.sncf.fab.myfirstproject.utils.AppConf._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.log4j.Logger

/** Created by simoh-labdoui on 10/05/2017. */
object CustomOptimizationExample {
  var LOGGER=Logger.getLogger(CustomOptimizationExample.getClass)
  object MultiplyOptimizationRule extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case Multiply(left, right) if right.isInstanceOf[Literal] &&
        right.asInstanceOf[Literal].value.asInstanceOf[Double] == 1.0 =>

        LOGGER.info("optimization of one applied")
        left
    }
  }


  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master(SPARK_MASTER)
      .appName(SAMPLE)
      .getOrCreate()


    val df = sparkSession.read.option("header", "true").csv("src/main/resources/sales.csv")
    val multipliedDF = df.selectExpr("amountPaid * 1")
    LOGGER.info("before optimization")
    LOGGER.info(multipliedDF.queryExecution.optimizedPlan.numberedTreeString)

    //add our custom optimization
    sparkSession.experimental.extraOptimizations = Seq(MultiplyOptimizationRule)
    val multipliedDFWithOptimization = df.selectExpr("amountPaid * 1")
    LOGGER.info("after optimization")

    LOGGER.info(multipliedDFWithOptimization.queryExecution.optimizedPlan.numberedTreeString)
  }
}
