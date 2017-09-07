package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{TgaTgdCycleId, TgaTgdInput}
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.persistence.Persist
import com.sncf.fab.ppiv.utils.AppConf.LANDING_WORK
import com.sncf.fab.ppiv.utils.AppConf.STICKING_PLASTER
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
import java.nio.file.{Paths, Files}


/**
  * Created by ELFI03951 on 12/07/2017.
  */
object BuildCycleOver {
  def getCycleOver(dsTgaTgdInput: Dataset[TgaTgdInput],
                   sc: SparkContext,
                   sqlContext: SQLContext,
                   panneau: String,
                   startTimeToProcess: DateTime,
                   endTimeToProcess: DateTime,
                   reprise: Boolean): DataFrame = {

    // Groupement et création des cycleId (concaténation de gare + panneau + numeroTrain + heureDepart)
    // (cycle_id{gare,panneau,numeroTrain,heureDepart}, heureDepart, retard)
    val cycleIdList = buildCycles(dsTgaTgdInput, sqlContext, panneau)


    // Parmi les cyclesId généré précédemment on filtre ceux dont l'heure de départ est deja passé
    // On renvoie le même format de données (cycle_id{gare,panneau,numeroTrain,heureDepart}, heureDepart, retard)
    val cycleIdListOver = filterCycleOver(cycleIdList, sqlContext, startTimeToProcess, endTimeToProcess, reprise)

    //Load les evenements  du jour j. Le 5ème paramètre sert a définir la journée qui nous intéresse 0 = jour J
    if (!reprise)
    {
      val tgaTgdRawToDay = loadDataFullPeriod(sc, sqlContext, panneau, startTimeToProcess)
      // Pour chaque cycle terminé récupération des différents évènements au cours de la journée
      // sous la forme d'une structure (cycle_id | Array(TgaTgdInput)
      val tgaTgdCycleOver = getEventCycleId(tgaTgdRawToDay, cycleIdListOver, sqlContext, sc, panneau)

      tgaTgdCycleOver
    }
    else
    {
      val tgaTgdCycleOver = getEventCycleId(dsTgaTgdInput, cycleIdListOver, sqlContext, sc, panneau)
      tgaTgdCycleOver
    }


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
                      startTimeToProcess: DateTime,
                      endTimeToProcess: DateTime,
                     reprise: Boolean): Dataset[TgaTgdCycleId] = {

    import sqlContext.implicits._

    val heureLimiteCycleCommencant = Conversion.getDateTime(
      startTimeToProcess.getYear,
      startTimeToProcess.getMonthOfYear,
      startTimeToProcess.getDayOfMonth,
      Conversion.getHourDebutPlageHoraire(startTimeToProcess).toInt,
      0,
      0)

    val heureLimiteCycleFini = if (reprise) {
      Conversion.getDateTime(
      startTimeToProcess.getYear,
      startTimeToProcess.getMonthOfYear,
      startTimeToProcess.getDayOfMonth,
      Conversion.getHourFinPlageHoraire(endTimeToProcess).toInt,
      0,
      0)
    }
    else {
      Conversion.getDateTime(
        startTimeToProcess.getYear,
        startTimeToProcess.getMonthOfYear,
        startTimeToProcess.getDayOfMonth,
      Conversion.getHourFinPlageHoraire(startTimeToProcess).toInt,
      0,
      0)
    }



    val timestampLimiteCycleCommencant = Conversion.getTimestampWithLocalTimezone(heureLimiteCycleCommencant)
    val timestampLimiteCycleFini = Conversion.getTimestampWithLocalTimezone(heureLimiteCycleFini)




    //DEVLOGGER.info("Filtre sur les cycles dont l'heure de départ est comprise entre : " + heureLimiteCycleCommencant.toString() + " et " + heureLimiteCycleFini.toString() + "en prenant en compte le retard de chaque cycle")
    // On veut filtrer les cycles dont l'heure de départ est situé entre l'heure de début du traitement du batch et celle de fin
    val dataTgaTgdCycleOver = dsTgaTgdCycles
      // Filtre sur les cycles terminés après le début de la plage en intégrant le retard
       .filter( x => x.heure > timestampLimiteCycleCommencant || (x.retard != "" && (x.heure + x.retard.toInt * 60 > timestampLimiteCycleCommencant)))
      // Filtre sur les cycles terminés avant le début de la plage en intégrant le retard
       .filter( x => x.heure < timestampLimiteCycleFini || (x.retard != "" && (x.heure +x.retard.toInt *60 < timestampLimiteCycleFini)))

    dataTgaTgdCycleOver
  }


  // Une fonction globale pour le chargement des fichiers sur la période optimale avec deux sous cas de figure
  // Soit le train est un train normal et on charge les fichiers de la même journée
  // Soit le train est un passe nuit (affichage après 18h la veille et départ avant midi le lendemain) et on charge aussi les fichiers à partir de 18h
  def loadDataFullPeriod(sc: SparkContext,
                         sqlContext: SQLContext,
                         panneau: String,
                         timeToProcess: DateTime ): Dataset[TgaTgdInput] = {


    // Il me faut une liste de Path de 18h a J-1 à l'heure actuelle de j
    // Cela revient à s'intéresser à toutes les heures de -6 à l'heure actuelle
    val hoursListJ = 0 to Conversion.getHourFinPlageHoraire(timeToProcess).toInt
    val hoursListJMoins1 = 18 to 23




    val pathFileJ = hoursListJ.map(x => LANDING_WORK + Conversion.getYearMonthDay(timeToProcess) + "/" + panneau + "-" +
      Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.HourFormat(x) + ".csv")

    var pathAllFile = IndexedSeq[String]()
    if(STICKING_PLASTER != true){
      val pathFileJMoins1 = hoursListJMoins1.map(x => LANDING_WORK + Conversion.getYearMonthDay(timeToProcess.plusDays(-1)) + "/" + panneau + "-" +
        Conversion.getYearMonthDay(timeToProcess.plusDays(-1)) + "_" + Conversion.HourFormat(x) + ".csv")

      // Fusion des paths à télécharger
      pathAllFile = pathFileJMoins1.union(pathFileJ)
      // Chargement de tous les fichiers dans un dataset par fichier
    }
    else{
      pathAllFile = pathFileJ
    }


    val tgaTgdAllPerHour = pathAllFile.map( filePath => LoadData.loadTgaTgd(sqlContext, filePath.toString))

    // Fusion des datasets entre eux
    val tgaTgdAllPeriod= tgaTgdAllPerHour.reduce((x, y) => x.union(y))


    // On applique le sparadrap si besoin
    val tgaTgdStickingPlaster = if (STICKING_PLASTER == true) {
      Preprocess.applyStickingPlaster(tgaTgdAllPeriod, sqlContext)
    } else tgaTgdAllPeriod
    // On applique la validation
    val tgaTgdValidated = ValidateData.validateField(tgaTgdStickingPlaster, sqlContext)
    // Retour des fichiers validés
    tgaTgdValidated._1
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

    // return la table des cycles finis avec evenement groupés + la table des  des cycles finis  evenements non groupés
    groupedDfEventAsString

  }
}
