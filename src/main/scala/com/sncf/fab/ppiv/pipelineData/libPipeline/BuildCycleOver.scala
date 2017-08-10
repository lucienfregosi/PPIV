package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{TgaTgdCycleId, TgaTgdInput}
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.persistence.Persist
import com.sncf.fab.ppiv.utils.AppConf.LANDING_WORK
import com.sncf.fab.ppiv.utils.Conversion
import com.sncf.fab.ppiv.utils.Conversion.ParisTimeZone
import groovy.sql.DataSet
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, collect_list, collect_set, concat, lit}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.LongType
import org.apache.spark.storage.StorageLevel
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object BuildCycleOver {
  def getCycleOver(dsTgaTgdInput: Dataset[TgaTgdInput],
                   sc: SparkContext,
                   sqlContext: SQLContext,
                   panneau: String): DataFrame = {

    // Groupement et création des cycleId (concaténation de gare + panneau + numeroTrain + heureDepart)
    val cycleIdList = buildCycles(dsTgaTgdInput, sqlContext, panneau)

    // Parmi les cyclesId généré précédemment on filtre ceux dont l'heure de départ est deja passé
    val cycleIdListOver = filterCycleOver(cycleIdList, sqlContext)

    //Load les evenements  du jour j
    val tgaTgdRawToDay = loadTgaTgdCurrentDay(sc, sqlContext, panneau)

    //Load les evenements du jour j -1
    val tgaTgdRawYesterDay = loadTgaTgdYesterDay(sc, sqlContext, panneau)

    // Union des evenement  de jour j et jour j -1
    val tgaTgdRawAllDay = tgaTgdRawToDay.union(tgaTgdRawYesterDay)

    // Pour chaque cycle terminé récupération des différents évènements au cours de la journée
    val tgaTgdCycleOver =
      getEventCycleId(tgaTgdRawAllDay, cycleIdListOver, sqlContext, sc, panneau)

    tgaTgdCycleOver._1
  }

 // Fonction pour construire les cycles
  def buildCycles(dsTgaTgd: Dataset[TgaTgdInput],
                  sqlContext: SQLContext,
                  panneau: String): Dataset[TgaTgdCycleId] = {
    import sqlContext.implicits._

    // Création d'une table temportaire pour la requête SQL
    dsTgaTgd.toDF().registerTempTable("dataTgaTgd")

    // GroupBy pour déterminer le cycleId que l'on parse dans une classe créé pour l'occasion
    val dataTgaTgCycles = sqlContext
      .sql(
        "SELECT concat(gare,'" + panneau + "',num,heure) as cycle_id, first(heure) as heure," +
          " last(retard) as retard" +
          " from dataTgaTgd group by concat(gare, '" + panneau + "',num,heure)")
      .withColumn("heure", 'heure.cast(LongType))
      .as[TgaTgdCycleId]

    dataTgaTgCycles

  }

  // Fonction pour filtrer les cycles finis
  def filterCycleOver(dsTgaTgdCycles: Dataset[TgaTgdCycleId],
                      sqlContext: SQLContext): Dataset[TgaTgdCycleId] = {
    import sqlContext.implicits._

    val currentHoraire = Conversion.getDateTime(
      Conversion.nowToDateTime().getYear,
      Conversion.nowToDateTime().getMonthOfYear,
      Conversion.nowToDateTime().getDayOfMonth,
      Conversion.getHourMax(Conversion.nowToDateTime()).toInt,
      0,
      0)

    // Filtre sur les horaire de départ inférieur a l'heure actuelle
    val dataTgaTgdCycleOver = dsTgaTgdCycles.filter(x =>
      (x.retard != "" && Conversion
        .unixTimestampToDateTime(x.heure)
        .plusMinutes(x.retard.toInt)
        .getMillis < currentHoraire.getMillis) || (x.retard == "" && (Conversion
        .unixTimestampToDateTime(x.heure)
        .getMillis < currentHoraire.getMillis)))

    dataTgaTgdCycleOver
  }

  // Fonction pour aller chercher tous les évènements d'un cycle
  def getEventCycleId(tgaTgdRawAllDay: Dataset[TgaTgdInput],
                      dsTgaTgdCyclesOver: Dataset[TgaTgdCycleId],
                      sqlContext: SQLContext,
                      sc: SparkContext,
                      panneau: String): (DataFrame, DataFrame) = {


    import sqlContext.implicits._

    // Sur le dataset Complet de la journée création d'une colonne cycle_id2 en vue de la jointure
    val tgaTgdInputAllDay = tgaTgdRawAllDay
      .toDF()
      .withColumn("cycle_id2",
                  concat(col("gare"), lit(panneau), col("num"), col("heure")))

    // On joint les deux avec un left join pour garder seulement les cycles terminés et leurs évènements
    val dfJoin = dsTgaTgdCyclesOver
      .toDF()
      .select("cycle_id")
      .join(tgaTgdInputAllDay, $"cycle_id" === $"cycle_id2", "inner")

    // On concatène toutes les colonnes en une pour pouvoir les manipuler plus facilement (en spark 1.6 pas possible de recréer un tgaTgdInput dans le collect list)
    val dfeventsAsString = dfJoin
      .drop("cycle_id2")
      .distinct()
      .dropDuplicates()
      .select(
        $"cycle_id",
        concat(
          $"gare",
          lit(";"),
          $"maj",
          lit(";"),
          $"train",
          lit(";"),
          $"ordes",
          lit(";"),
          $"num",
          lit(";"),
          $"type",
          lit(";"),
          $"picto",
          lit(";"),
          $"attribut_voie",
          lit(";"),
          $"voie",
          lit(";"),
          $"heure",
          lit(";"),
          $"etat",
          lit(";"),
          $"retard"
        ) as "event"
      )

    // collect set: la fonction qui regroupe les evenements  qui appartiennent au meme cycle Id
    def collectSet(df: DataFrame, k: Column, v: Column): DataFrame = {
      val transformedDf= df
        .select(k.as("k"), v.as("v"))
        .map(r => (r.getString(0), r.getString(1)))
        .reduceByKey((x, y) => x + "," + y)
        .toDF("cycle_id", "event")
      transformedDf
    }
    // application de la fonction collect set sur la table dfeventsGrouped
    val testCollection = collectSet(dfeventsAsString,
                                    dfeventsAsString("cycle_id"),
                                    dfeventsAsString("event"))
    //suppression des lignes en double
    val testCollectionWithoutDuplica = testCollection.distinct()

    // return la table des cycles finis avec evenement groupés + la table des  des cycles finis  evenements non groupés
    (testCollectionWithoutDuplica, dfJoin)

  }

    /**
    *Fonction pour charger les données de toute la journée1
    * @param sc
    * @param sqlContext
    * @param panneau
    * @return La table des evenement de toute la journée
    */
  def loadTgaTgdCurrentDay(sc: SparkContext,
                           sqlContext: SQLContext,
                           panneau: String): Dataset[TgaTgdInput] = {

    import sqlContext.implicits._

    // Déclaration de notre variable de sortie contenant tous les event de la journée
    var tgaTgdRawAllDay = sc.emptyRDD[TgaTgdInput].toDS()

    // Définition de l'heure actuelle que l'on a processé
    // TODO: Modifier par une variable globale
    val currentHourInt = Conversion.getHour(Conversion.nowToDateTime()).toInt

    // LOOP Over all Files of the current day from midnight to CurrentHour
    for (loopHour <- 0 to currentHourInt) {

      // Créatipon du nom du fichier dans HDFS
      var filePath = LANDING_WORK + Conversion.getYearMonthDay(
        Conversion.nowToDateTime()) + "/" + panneau + "-" + Conversion
        .getYearMonthDay(Conversion.nowToDateTime()) + "_" + Conversion
        .HourFormat(loopHour) + ".csv"


      // Chargement effectif du fichier
      val tgaTgdHour = LoadData.loadTgaTgd(sqlContext, filePath)

      // Ajout dans notre variabel de sortie
      tgaTgdRawAllDay = tgaTgdRawAllDay.union(tgaTgdHour)
    }
    tgaTgdRawAllDay
  }

  /**
    *Fonction pour chercher les evenements du jour -1
    * @param sc
    * @param sqlContext
    * @param panneau
    * @return La table des evenement du j-1
    */
  def loadTgaTgdYesterDay(sc: SparkContext,
                          sqlContext: SQLContext,
                          panneau: String): Dataset[TgaTgdInput] = {

    import sqlContext.implicits._

    // Déclaration de notre variable de sortie contenant tous les event de la journée
    var tgaTgdRawAllDay = sc.emptyRDD[TgaTgdInput].toDS()

    // LOOP Over all Files of  yesterday
    for (loopHour <- 0 to 23) {

      // Créatipon du nom du fichier dans HDFS
      var filePath = LANDING_WORK + Conversion.getYearMonthDay(
        Conversion
          .nowToDateTime()
          .plusDays(-1)) + "/" + panneau + "-" + Conversion.getYearMonthDay(
        Conversion.nowToDateTime().plusDays(-1)) + "_" + Conversion.HourFormat(
        loopHour) + ".csv"

      // Chargement effectif du fichier
      val tgaTgdHour = LoadData.loadTgaTgd(sqlContext, filePath)

      // Ajout dans notre variabel de sortie
      tgaTgdRawAllDay = tgaTgdRawAllDay.union(tgaTgdHour)
    }
    tgaTgdRawAllDay
  }

}
