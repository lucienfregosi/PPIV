package com.sncf.fab.ppiv.pipelineData

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.business.{ReferentielGare, TgaTgdInput, TgaTgdOutput, TgaTgdTransitionnal}
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.persistence.{PersistElastic, PersistHdfs, PersistHive, PersistLocal}
import com.sncf.fab.ppiv.utils.AppConf._
import org.apache.spark.SparkConf
import com.sncf.fab.ppiv.utils.Conversion
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
trait SourcePipeline extends Serializable {


  @transient val sparkConf = getSparkConf()
   @transient val sc = new SparkContext(sparkConf)
  @transient val sqlContext = new SQLContext(sc)
  def getSparkConf() : SparkConf = {
    new SparkConf()
      .setAppName(PPIV)
      .setMaster(SPARK_MASTER)
      .set("es.nodes", HOST)
      .set("es.port", "9201")
      .set("es.index.auto.create", "true")
  }




  /**
    *
    * @return le nom de l'application spark visible dans historyserver
    */

  def getAppName(): String = {
    PPIV
  }

  /**
    * Le fichier source n'a pas de header mais possède le format suivant
    * (0) -> gare
    * (1) -> maj
    * (2) -> train
    * (3) -> ordes
    * (4) -> num
    * (5) -> type
    * (6) -> picto
    * (7) -> attribut_voie
    * (8) -> voie
    * (9) -> heure
    * (10) -> etat
    * (11) -> retard
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

  def start(outputs: Array[String]): Unit = {
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


    val newNamesRefGares = Seq("CodeGare","IntituleGare","NombrePlateformes","SegmentDRG","UIC","UniteGare","TVS","CodePostal","Commune","DepartementCommune","Departement","Region","AgenceGC","RegionSNCF","NiveauDeService","LongitudeWGS84","LatitudeWGS84","DateFinValiditeGare")
    val refGares = sqlContext.read
      .option("delimiter", ";")
      .option("header", "true")
      .option("charset", "UTF8")
      .format("com.databricks.spark.csv")
      .load(REFINERY_HDFS + REF_GARES)
      .toDF(newNamesRefGares: _*)
      .as[ReferentielGare]


    val dataTgaTgd = dsTgaTgd.toDF().map(row => DatasetsParser.parseTgaTgdDataset(row)).toDS()

    val dataRefGares = refGares.toDF().map(DatasetsParser.parseRefGares).toDS()

    //dataTgaTgd.show
    //dataRefGares.show

    // On groupe les cycles entre eux
    dataTgaTgd.toDF().registerTempTable("dataTgaTgd")
    val dataTgaTgdGrouped = sqlContext.sql("SELECT concat(gare,num,'TGA',heure) as cycle_id, first(heure) as heure," +
      " first(gare) as gare, first(num) as num_train, first(type) as type, first(ordes) as origine_destination" +
      " from dataTgaTgd group by concat(gare,num,'TGA',heure)")
      .as[TgaTgdTransitionnal]


    process(dataTgaTgdGrouped, dataRefGares, outputs)
  }

  /**
    *
    * @param dsTgaTgd le dataset issu des fichier TGA/TGD (Nettoyé)
    */
  def process(dsTgaTgd: Dataset[TgaTgdTransitionnal], refGares: Dataset[ReferentielGare], outputs: Array[String]): Unit = {
    import sqlContext.implicits._
    try {


      // Jointure après le calcul de la règle de gestion
      //val qualiteAffichage = joinData(dsTgaTgd, refGares)
      val finals = dsTgaTgd.toDF().join(refGares.toDF(), dsTgaTgd.toDF().col("gare") === refGares.toDF().col("TVS"))

      val affichageFinal = finals.toDF().map(row => TgaTgdOutput(row.getString(7), row.getString(18),
        row.getString(9), row.getString(10),
        row.getString(21), row.getString(22),row.getString(0),row.getString(3),row.getString(4),
        row.getString(5), Panneau(), Conversion.unixTimestampToDateTime(row.getLong(9)).toString
      ))

      val qualiteAffichage = affichageFinal.toDS().as[TgaTgdOutput]

      qualiteAffichage.show()


      //PersistHdfs.persisteQualiteAffichageIntoHdfs(qualiteAffichage, getOutputRefineryPath())

      /*
      // Enregistrement du résultat sur le serveur
      if (outputs.contains("fs"))
        PersistLocal.persisteTgaTgdParsedIntoFs(dsTgaTgd, getOutputRefineryPath())
      if (outputs.contains("hive"))
        PersistHive.persisteTgaTgdParsedHive(dsTgaTgd)
      if (outputs.contains("hdfs"))
        PersistHdfs.persisteQualiteAffichageIntoHdfs(qualiteAffichage, GOLD_HDFS)
      if (outputs.contains("es"))
        PersistElastic.persisteQualiteAffichageIntoEs(qualiteAffichage, QUALITE_INDEX)

      */

    }
    catch {
      case e: Throwable => {
        e.printStackTrace()
        PpivRejectionHandler.handleRejection(e.getMessage, PpivRejectionHandler.PROCESSING_ERROR)
        None
      }
    }

  }

  /**
    * Jointure avec RefGares
    *
    * @param dsTgaTgd issu des fichiers sources TGA/TGD
    * @param refGares issu des fichiers sources refGares
    */
  def joinData(dsTgaTgd: Dataset[TgaTgdTransitionnal], refGares: Dataset[ReferentielGare]): Dataset[TgaTgdOutput] = {
    import sqlContext.implicits._

    // Jointure avec le référentiel
    val finals = dsTgaTgd.toDF().join(refGares.toDF(), dsTgaTgd.toDF().col("gare") === refGares.toDF().col("TVS"))

    val affichageFinal = finals.toDF().map(row => TgaTgdOutput(row.getString(7), row.getString(18),
      row.getString(9), row.getString(10),
      row.getString(21), row.getString(22),row.getString(0),row.getString(3),row.getString(4),
      row.getString(5), Panneau(), Conversion.unixTimestampToDateTime(row.getLong(9)).toString
    ))

    affichageFinal.toDS().as[TgaTgdOutput]
  }


}



