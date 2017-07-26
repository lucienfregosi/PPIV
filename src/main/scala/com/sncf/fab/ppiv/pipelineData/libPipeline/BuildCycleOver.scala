package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{TgaTgdCycleId, TgaTgdInput}
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.utils.AppConf.LANDING_WORK
import com.sncf.fab.ppiv.utils.Conversion
import com.sncf.fab.ppiv.utils.Conversion.ParisTimeZone
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, collect_list, concat, lit}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.LongType
import org.joda.time.{DateTime, DateTimeZone}


/**
  * Created by ELFI03951 on 12/07/2017.
  */
object BuildCycleOver {

  def getCycleOver(dsTgaTgdInput: Dataset[TgaTgdInput],sc: SparkContext, sqlContext: SQLContext, panneau: String): DataFrame = {

    // Groupement et création des cycleId (concaténation de gare + panneau + numeroTrain + heureDepart)
    val cycleIdList       = buildCycles(dsTgaTgdInput, sqlContext, panneau)

    println ("-------------------- Count of all cycles :" + cycleIdList.count())

    // Parmi les cyclesId généré précédemment on filtre ceux dont l'heure de départ est deja passé
    val cycleIdListOver   = filterCycleOver(cycleIdList, sqlContext)

    println ("-------------------- Count of all Finished cycles :" + cycleIdListOver.count())

    // Pour chaque cycle terminé récupération des différents évènements au cours de la journée
    val tgaTgdCycleOver   = getEventCycleId(cycleIdListOver, sqlContext, sc, panneau)

    tgaTgdCycleOver
  }

  def buildCycles(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext, panneau: String) : Dataset[TgaTgdCycleId] = {
    import sqlContext.implicits._

    // Création d'une table temportaire pour la requête SQL
    dsTgaTgd.toDF().registerTempTable("dataTgaTgd")

    // GroupBy pour déterminer le cycleId que l'on parse dans une classe créé pour l'occasion
    val dataTgaTgCycles = sqlContext.sql("SELECT concat(gare,'" + panneau + "',num,heure) as cycle_id, first(heure) as heure," +
      " last(retard) as retard" +
      " from dataTgaTgd group by concat(gare, '" + panneau + "',num,heure)")
      .withColumn("heure", 'heure.cast(LongType))
      .as[TgaTgdCycleId]

    dataTgaTgCycles
  }

  def filterCycleOver(dsTgaTgdCycles : Dataset[TgaTgdCycleId], sqlContext : SQLContext):  Dataset[TgaTgdCycleId]= {
    import sqlContext.implicits._

    val currentHoraire = Conversion.getDateTime(2017,7,26,Conversion.getHourMax(Conversion.nowToDateTime()).toInt,0,0)

    //println (" currentHoraire :" + Conversion.dateTimeToString(currentHoraire))

    // Filtre sur les horaire de départ inférieur a l'heure actuelle
    val dataTgaTgdCycleOver = dsTgaTgdCycles.filter( x => ( x.retard != "" &&  Conversion.getDateTime(x.heure).plusMinutes(x.retard.toInt).getMillis < currentHoraire.getMillis) ||(x.retard == "" &&  (Conversion.getDateTime(x.heure).getMillis < currentHoraire.getMillis )))


    dataTgaTgdCycleOver
  }

  // Fonction pour aller chercher tous les évènements d'un cycle
  def getEventCycleId(dsTgaTgdCyclesOver : Dataset[TgaTgdCycleId], sqlContext : SQLContext, sc : SparkContext, panneau: String): DataFrame = {

    // Définition d'un Hive Context pour utiliser la fonction collect_list
    val hiveContext = new HiveContext(sc)
    import sqlContext.implicits._

    // Déclaration de notre variable de sortie contenant tous les event de la journée
    var tgaTgdRawAllDay = sc.emptyRDD[TgaTgdInput].toDS()

    // Définition de l'heure actuelle que l'on a processé
    // TODO: Modifier par une variable globale
    val currentHourString = Conversion.getHour(Conversion.nowToDateTime())
    val currentHourInt = Conversion.getHour(Conversion.nowToDateTime()).toInt

    // LOOP Over all Files of the current day from midnight to CurrentHour
    for (loopHour <- 1 to currentHourInt) {


      // Créatipon du nom du fichier dans HDFS
      var filePath = LANDING_WORK + Conversion.getYearMonthDay(Conversion.nowToDateTime()) + "/" + panneau + "-" + Conversion
          .getYearMonthDay(Conversion.nowToDateTime()) + "_" + currentHourString + ".csv"

      // Chargement effectif du fichier
      val tgaTgdHour = LoadData.loadTgaTgd(sqlContext, filePath)

      // Ajout dans notre variabel de sortie
      tgaTgdRawAllDay = tgaTgdRawAllDay.union(tgaTgdHour)
    }

    // Sur le dataset Complet de la journée création d'une colonne cycle_id2 en vue de la jointure
    val tgaTgdInputAllDay = tgaTgdRawAllDay.toDF().withColumn("cycle_id2", concat(col("gare"), lit(panneau), col("num"), col("heure")))

    // On joint les deux avec un left join pour garder seulement les cycles terminés et leurs évènements
    val dfJoin = dsTgaTgdCyclesOver.toDF().select("cycle_id").join(tgaTgdInputAllDay, $"cycle_id" === $"cycle_id2", "left")

    // Création d'une dataframe hive pour pouvoir utiliser la fonction collect_list
    val hiveDataframe = hiveContext.createDataFrame(dfJoin.rdd, dfJoin.schema)

    // On concatène toutes les colonnes en une pour pouvoir les manipuler plus facilement (en spark 1.6 pas possible de recréer un tgaTgdInput dans le collect list
    val dfGroupByCycleOver = hiveDataframe.drop("cycle_id2").distinct().select($"cycle_id", concat($"gare", lit(","), $"maj", lit(","), $"train", lit(","), $"ordes", lit(","), $"num", lit(","), $"type", lit(","), $"picto", lit(","), $"attribut_voie", lit(","), $"voie", lit(","), $"heure", lit(","), $"etat", lit(","), $"retard") as "event")
      .groupBy("cycle_id").agg(
      collect_list($"event") as "event"
    )

    dfGroupByCycleOver
  }

}
