package com.sncf.fab.ppiv.pipelineData

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.business.{ReferentielGare, TgaTgdInput, TgaTgdOutput, TgaTgdTransitionnal}
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.utils.AppConf._
import org.apache.spark.SparkConf
import com.sncf.fab.ppiv.utils.Conversion
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import com.sncf.fab.ppiv.persistence.{PersistElastic, PersistHdfs, PersistHive, PersistLocal}
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
trait SourcePipeline extends Serializable {


  /**
    *
    * @return le nom de l'application spark visible dans historyserver
    */

  def getAppName(): String = {
    PPIV
  }

  /**
    * @return le chemin de la source de données brute
    */
  def getSource(): String
  /**
    *
    * @return le chemin de l'output qualité
    */
  def getOutputGoldPath(): String

  /**
    *
    * @return the path used to store the cleaned TgaTgaPased
    */

  def getOutputRefineryPath(): String


  /**
    *
    * @return vrai s'il s'agit d'un départ de train, faux s'il s'agit d'un arrivé
    */
  def Depart(): Boolean

  /**
    *
    * @return faux s'il s'agit d'un départ de train, vrai s'il s'agit d'un arrivé
    */
  def Arrive(): Boolean

  /**
    *
    * @return TGA ou TGD selon le type de trajet
    */
  def Panneau(): String


  /**
    * le traitement principal lancé pour chaque data source
    */

  def start(outputs: Array[String], sc : SparkContext, sqlContext : SQLContext): Dataset[TgaTgdOutput] = {

    // Début du Pipeline

    // 1) Chargement des fichiers déjà parsé dans leur classe
    val dataTgaTgd                = loadTgaTgd(sqlContext)
    val dataRefGares              = loadReferentiel(sqlContext)

    // 2) Application du sparadrap sur les données au cause du Bug lié au passe nuit
    val dataTgaTgdBugFix          = applyStickingPaser(dataTgaTgd, sqlContext)

    // 3) Validation champ à champ
    val dataTgaTgdFielValidated   = validateField(dataTgaTgdBugFix, sqlContext)

    // 4) Regroupement en cycle
    val dataTgaTgdGrouped         = groupDataByCycle(dataTgaTgdFielValidated, sqlContext)

    // 5) Sélection des lignes dont les cycles sont terminé et enrichissement dans les fichiers horaires précédents
    val dataTgaTgdCycleOver       = filterCycleOver(dataTgaTgdGrouped, sqlContext)

    // 6) Validation des cycles
    val dataTgaTgdCycleValidated  = validateCycle(dataTgaTgdCycleOver, sqlContext)

    // 7) Nettoyage et mise en forme
    val dataTgaTgdCycleCleaned    = cleanCycle(dataTgaTgdCycleValidated, sqlContext)

    // 8) Sauvegarde des données propres la ou G&C le souhaite
    saveCleanData(dataTgaTgdCycleCleaned, sqlContext)

    // 9) Calcul des différents cas d'exceptions
    val dataTgaTgdCycleKPI        = computeKPI(dataTgaTgdCycleCleaned, sqlContext)

    // 10) Jointure avec le référentiel
    val dataTgaTgdWithReferentiel = joinReferentiel(dataTgaTgdCycleKPI, dataRefGares,sqlContext )

    // Reste l'enregistrement que l'on fait a la fin du traitement TGA et TGD (donc un cran plus haut)
    dataTgaTgdWithReferentiel

  }


  def loadTgaTgd(sqlContext : SQLContext): Dataset[TgaTgdInput] = {
    import sqlContext.implicits._
    // Comme pas de header définition du nom des champs
    val newNamesTgaTgd = Seq("gare","maj","train","ordes","num","type","picto","attribut_voie","voie","heure","etat","retard","null")
    // Lecture du CSV avec les bons noms de champs
    val dsTgaTgd = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", ";")
      .load(getSource()).toDF(newNamesTgaTgd: _*)
      .withColumn("maj", 'maj.cast(LongType))
      .withColumn("heure", 'heure.cast(LongType))
      .as[TgaTgdInput];

   dsTgaTgd.toDF().map(row => DatasetsParser.parseTgaTgdDataset(row)).toDS()
  }

  def loadReferentiel(sqlContext : SQLContext) : Dataset[ReferentielGare] = {
    import sqlContext.implicits._
    val newNamesRefGares = Seq("CodeGare","IntituleGare","NombrePlateformes","SegmentDRG","UIC","UniteGare","TVS","CodePostal","Commune","DepartementCommune","Departement","Region","AgenceGC","RegionSNCF","NiveauDeService","LongitudeWGS84","LatitudeWGS84","DateFinValiditeGare")
    val refGares = sqlContext.read
      .option("delimiter", ";")
      .option("header", "true")
      .option("charset", "UTF8")
      .format("com.databricks.spark.csv")
      .load(REF_GARES)
      .toDF(newNamesRefGares: _*)
      .as[ReferentielGare]

    refGares.toDF().map(DatasetsParser.parseRefGares).toDS()
  }


  def applyStickingPaser(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext): Dataset[TgaTgdInput] = {
    import sqlContext.implicits._
    // Application du sparadrap ...
    dsTgaTgd
  }

  def validateField(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext): Dataset[TgaTgdInput] = {
    import sqlContext.implicits._
    // Validation de chaque champ avec les contraintes définies dans le dictionnaire de données
    // Voir comment traiter les rejets ..
    val currentTimestamp = DateTime.now(DateTimeZone.UTC).getMillis() / 1000

     dsTgaTgd.show()
     val dsTgaTgdValidatedFields = dsTgaTgd.toDF().filter($"gare" rlike "^[A-Z]{3}$").filter( $"maj" <= currentTimestamp ).filter( $"train" rlike "^[0-2]{0,1}[0-9]$")
       .filter($"ordes" rlike "^[A-Z]{1,}$").filter($"num" rlike "^[0-9]{1,}$").filter($"num" .cast(IntegerType) >=0).filter($"type" isin ("TER","BUS","TGV","INTERCITES"))
       .filter($"picto".cast(IntegerType) >=0).filter($"attribut_voie" isin ("", "I")).filter($"voie" rlike "^[0-9|A-Z]{1}$").filter($"heure" <= currentTimestamp)
       .filter($"etat" isin ("IND", "SUP","ARR", "")).filter($"retard" rlike "^[[0-9]{2}|[0-9]{4}]{0,1}$")
   
      dsTgaTgdValidatedFields.show()

      dsTgaTgd

  }


  def groupDataByCycle(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext): Dataset[TgaTgdTransitionnal] = {
    import sqlContext.implicits._
    // Groupement des évènements pour constituer des cycles uniques concaténation de gare + panneau + numéro de train + heure de départ (timestamp)
    dsTgaTgd.toDF().registerTempTable("dataTgaTgd")
    val dataTgaTgdGrouped = sqlContext.sql("SELECT concat(gare,num,'TGA',heure) as cycle_id, first(heure) as heure," +
      " first(gare) as gare, first(num) as num_train, first(type) as type, first(ordes) as origine_destination" +
      " from dataTgaTgd group by concat(gare,num,'TGA',heure)")
      .withColumn("heure", 'heure.cast(LongType))
      .as[TgaTgdTransitionnal]

    dataTgaTgdGrouped
  }

  def filterCycleOver(dsTgaTgd: Dataset[TgaTgdTransitionnal], sqlContext : SQLContext): Dataset[TgaTgdTransitionnal] = {
    import sqlContext.implicits._
    // Sélection des trajets finis (voir combien de temps après le départ on le considère comme fini)
    // Puis récupération des évènements antérieurs pour les cycles définis comme finis.
    dsTgaTgd
  }

  def validateCycle(dsTgaTgd: Dataset[TgaTgdTransitionnal], sqlContext : SQLContext): Dataset[TgaTgdTransitionnal] = {
    import sqlContext.implicits._
    // Validation des cycles. Un cycle doit comporter au moins une voie et tous ses évènements ne peuvent pas se passer x minutes après le départ du train
    // Voir comment traiter les rejets ..
    dsTgaTgd
  }

  def cleanCycle(dsTgaTgd: Dataset[TgaTgdTransitionnal], sqlContext : SQLContext): Dataset[TgaTgdTransitionnal] = {
    import sqlContext.implicits._
    // Nettoyage, mise en forme des lignes, conversion des heures etc ..
    dsTgaTgd
  }

  def saveCleanData(dsTgaTgd: Dataset[TgaTgdTransitionnal], sqlContext : SQLContext): Unit = {
    import sqlContext.implicits._
    // Sauvegarde des données pour que G&C ait un historique d'Obier exploitable
    None
  }

  def computeKPI(dsTgaTgd: Dataset[TgaTgdTransitionnal], sqlContext : SQLContext): Dataset[TgaTgdTransitionnal] = {
    import sqlContext.implicits._
    // Calcul des différents indicateurs
    // On devra surement spliter la fonction en différentes sous fonctions
    dsTgaTgd
  }


  def joinReferentiel(dsTgaTgd: Dataset[TgaTgdTransitionnal], refGares : Dataset[ReferentielGare], sqlContext : SQLContext): Dataset[TgaTgdOutput] = {
    // Jointure avec le référentiel pour enrichir les lignes
    import sqlContext.implicits._

    val joinedData = dsTgaTgd.toDF().join(refGares.toDF(), dsTgaTgd.toDF().col("gare") === refGares.toDF().col("TVS"))

    val affichageFinal = joinedData.toDF().map(row => TgaTgdOutput(row.getString(7), row.getString(18),
      row.getString(9), row.getString(10),
      row.getString(21), row.getString(22),row.getString(0),row.getString(3),row.getString(4),
      row.getString(5), Panneau(), Conversion.unixTimestampToDateTime(row.getLong(1)).toString
    ))

    affichageFinal.toDS().as[TgaTgdOutput]


  }
}



