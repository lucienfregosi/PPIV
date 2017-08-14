package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{TgaTgdCycleId, TgaTgdInput}
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.persistence.Persist
import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver.{DEVLOGGER, MAINLOGGER}
import com.sncf.fab.ppiv.utils.AppConf.{LANDING_WORK, STICKING_PLASTER}
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
                   panneau: String,
                   timeToProcess: DateTime): DataFrame = {

    // Groupement et création des cycleId (concaténation de gare + panneau + numeroTrain + heureDepart)
    // (cycle_id{gare,panneau,numeroTrain,heureDepart}, heureDepart, retard)
    val cycleIdList = buildCycles(dsTgaTgdInput, sqlContext, panneau)

    // Parmi les cyclesId généré précédemment on filtre ceux dont l'heure de départ est deja passé
    // On renvoie le même format de données (cycle_id{gare,panneau,numeroTrain,heureDepart}, heureDepart, retard)
    val cycleIdListOver = filterCycleOver(cycleIdList, sqlContext, timeToProcess)
    println("Nombre de cyle terminé: " + cycleIdListOver.count())
    println("Nombre de cyle terminé DISTINCT: " + cycleIdListOver.distinct.count())
    println("Pourcentage de cyle terminé: " + (cycleIdListOver.count() / cycleIdList.count())*100 + "%")
    DEVLOGGER.info("Nombre de cyle terminé: " + cycleIdListOver.count())
    DEVLOGGER.info("Nombre de cyle terminé DISTINCT: " + cycleIdListOver.distinct.count())
    DEVLOGGER.info("Pourcentage de cyle terminé: " + (cycleIdListOver.count() / cycleIdList.count())*100 + "%")

    //Load les evenements  du jour j. Le 5ème paramètre sert a définir la journée qui nous intéresse 0 = jour J
    val tgaTgdRawToDay = loadDataEntireDay(sc, sqlContext, panneau, timeToProcess, 0)

    //Load les evenements du jour j -1. Le 5ème paramètre sert a définir la journée qui nous intéresse -1 = jour J-1
    // TODO : Amélioration :
    // - à J-1 aller chercher les données seulement à partir de 18h
    // - Appeler cette fonction uniquement dans le case des trains passe nuits (qui partent avant 12)
    val tgaTgdRawYesterDay = loadDataEntireDay(sc, sqlContext, panneau, timeToProcess, -1)

    // Union des evenement  de jour j et jour j -1
    val tgaTgdRawAllDay = tgaTgdRawToDay.union(tgaTgdRawYesterDay)
    DEVLOGGER.info("Nombre de lignes chargé sur la journée et/ou j-1 (si passe nuit) :" + tgaTgdRawAllDay.count())


    // TODO: Ajout d'une étape nettoyage (sparadrap + validation champ a champ (sans enregistrement des rejets)
    // Pour chaque cycle terminé récupération des différents évènements au cours de la journée
    // sous la forme d'une structure (cycle_id | Array(TgaTgdInput)
    val tgaTgdCycleOver = getEventCycleId(tgaTgdRawAllDay, cycleIdListOver, sqlContext, sc, panneau)
    DEVLOGGER.info("Nombre de cycle enrichi avec les événement de j et/ou j-1: " + tgaTgdCycleOver.count())
    DEVLOGGER.info("Nombre de cycle enrichi avec les événement de j et/ou j-1 DISTINCT: " + tgaTgdCycleOver.distinct().count())


    tgaTgdCycleOver
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


  // TODO faire passer l'heure a jouer en paramètre

  def filterCycleOver(dsTgaTgdCycles: Dataset[TgaTgdCycleId],
                      sqlContext: SQLContext,
                      timeToProcess: DateTime): Dataset[TgaTgdCycleId] = {
    import sqlContext.implicits._

    val heureLimiteCycleCommencant = Conversion.getDateTime(
      timeToProcess.getYear,
      timeToProcess.getMonthOfYear,
      timeToProcess.getDayOfMonth,
      Conversion.getHourDebutPlageHoraire(timeToProcess).toInt,
      0,
      0)

    val heureLimiteCycleFini = Conversion.getDateTime(
      timeToProcess.getYear,
      timeToProcess.getMonthOfYear,
      timeToProcess.getDayOfMonth,
      Conversion.getHourFinPlageHoraire(timeToProcess).toInt,
      0,
      0)


    DEVLOGGER.info("Filtre sur les cycles dont l'heure de départ est comprise entre : " + heureLimiteCycleCommencant.toString() + " et " + heureLimiteCycleFini.toString() + "en prenant en compte le retard de chaque cycle")


    // Filtre sur les horaire de départ inférieur a l'heure de fin de plage
    // et sur les horaires de départ supérieur a l'heure de début de plage
    val dataTgaTgdCycleOver = dsTgaTgdCycles.filter(x =>
      (
        (
           // Cas avec retard on prend en compte le retard pour voir si le train est déja parti
            x.retard != "" &&
            Conversion.unixTimestampToDateTime(x.heure).plusMinutes(x.retard.toInt).getMillis < heureLimiteCycleFini.getMillis
          ) ||
          (
            // Cas sans retard on se tient a la date de départ théorique du train
            x.retard == "" &&
              Conversion.unixTimestampToDateTime(x.heure).getMillis < heureLimiteCycleFini.getMillis
            )
        &&
            (
              // Cas avec retard on prend en compte le retard pour voir si le train est déja parti
              x.retard != "" &&
                Conversion.unixTimestampToDateTime(x.heure).plusMinutes(x.retard.toInt).getMillis > heureLimiteCycleCommencant.getMillis
              ) ||
          (
            // Cas sans retard on se tient a la date de départ théorique du train
            x.retard == "" &&
              Conversion.unixTimestampToDateTime(x.heure).getMillis > heureLimiteCycleCommencant.getMillis
            )
        )
    )
    dataTgaTgdCycleOver
  }



  def loadDataEntireDay(sc: SparkContext,
                           sqlContext: SQLContext,
                           panneau: String,
                           timeToProcess: DateTime,
                           dayBeforeToProcess: Int): Dataset[TgaTgdInput] = {

    import sqlContext.implicits._

    // Déclaration de notre variable de sortie contenant tous les event de la journée
    // TODO Peut etre optimisable pour éviter 24 append
    var tgaTgdRawAllDay = sc.emptyRDD[TgaTgdInput].toDS()

    // 2 cas de figure :
    // - Le 5ème argument vaut 0, on va chercher dans les évènements de la journée, on s'arrête a l'heure actuelle
    // - Le 5ème argument inférieur à 0, on va chercher dans les jours précédents, on process toutes les heures de la journée
    val currentHourInt = if(dayBeforeToProcess == 0) Conversion.getHourDebutPlageHoraire(timeToProcess).toInt else 23

    // Boucle sur les heures de la journée à traiter
    for (loopHour <- 0 to currentHourInt) {

      // Construction du nom du fichier a aller chercher dans HDFS
      var filePath = LANDING_WORK + Conversion.getYearMonthDay(timeToProcess.plusDays(dayBeforeToProcess)) + "/" + panneau + "-" +
        Conversion.getYearMonthDay(timeToProcess.plusDays(dayBeforeToProcess)) + "_" + Conversion.HourFormat(loopHour) + ".csv"

      // Chargement effectif du fichier
      val tgaTgdHour = LoadData.loadTgaTgd(sqlContext, filePath)

      // Nettoyage rapide du fichier, application du sparadrap si besoin et validation champ à champ
      val tgaTgdHourStickingParser = if (STICKING_PLASTER == true) {
        MAINLOGGER.info("Flag sparadrap activé, application de la correction")
        Preprocess.applyStickingPlaster(tgaTgdHour, sqlContext)
      } else tgaTgdHour

      val tgaTgdHourUseful = ValidateData.validateField(tgaTgdHourStickingParser, sqlContext)

      // Ajout dans notre variable de sortie
      tgaTgdRawAllDay = tgaTgdRawAllDay.union(tgaTgdHourUseful._1)
    }
    tgaTgdRawAllDay
  }

  // Fonction pour aller chercher tous les évènements d'un cycle
  def getEventCycleId(tgaTgdRawAllDay: Dataset[TgaTgdInput],
                      dsTgaTgdCyclesOver: Dataset[TgaTgdCycleId],
                      sqlContext: SQLContext,
                      sc: SparkContext,
                      panneau: String): DataFrame = {


    import sqlContext.implicits._

    // Sur le dataset Complet de la journée création d'une colonne cycle_id2 en vue de la jointure
    val tgaTgdInputAllDay = tgaTgdRawAllDay
      .toDF()
      .withColumn("cycle_id2",
        concat(col("gare"), lit(panneau), col("num"), col("heure")))

    // On joint les deux avec un inner join pour garder seulement les cycles terminés et leurs évènements
    // On se retrouve avec une structure de la forme (cycle_Id | TgaTgdInput)
    DEVLOGGER.info("Nombre de cycles terminés: " + dsTgaTgdCyclesOver.count())
    val dfJoin = dsTgaTgdCyclesOver
      .toDF()
      .select("cycle_id")
      .join(tgaTgdInputAllDay, $"cycle_id" === $"cycle_id2", "inner")





    // TODO : En parler a Mohamed
    // On concatène toutes les colonnes en une pour pouvoir les manipuler plus facilement (en spark 1.6 pas possible de recréer un tgaTgdInput dans le collect list)
    val dfeventsAsString = dfJoin
      .drop("cycle_id2")
      .distinct()
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
    def collectList(df: DataFrame, k: Column, v: Column): DataFrame = {
      val transformedDf= df
        .select(k.as("k"), v.as("v"))
        .map(r => (r.getString(0), r.getString(1)))
        .reduceByKey((x, y) => x + "," + y)
        .toDF("cycle_id", "event")
      transformedDf
    }
    // application de la fonction collect set sur la table dfeventsGrouped
    val groupedDfEventAsString = collectList(dfeventsAsString,
      dfeventsAsString("cycle_id"),
      dfeventsAsString("event"))

    DEVLOGGER.info("Nombre de cycles terminés et enrichis avec les tgatgd de la journée : " + groupedDfEventAsString.count())

    // return la table des cycles finis avec evenement groupés + la table des  des cycles finis  evenements non groupés
    groupedDfEventAsString

  }
}
