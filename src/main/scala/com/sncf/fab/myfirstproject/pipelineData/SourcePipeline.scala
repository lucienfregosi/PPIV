package com.sncf.fab.myfirstproject.pipelineData
import com.sncf.fab.myfirstproject.business.TgaTgd
import com.sncf.fab.myfirstproject.persistence.PersistTgaTgd
import com.sncf.fab.myfirstproject.utils.AppConf._
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
trait SourcePipeline extends Serializable{


  /**
    *
    * @return le nom de l'application spark visible dans historyserver
    */
  def getAppName(): String = {
  PPIV
  }

  /**
    *
    * @return le chemin de la source de données brute
    */
  def getSource():String

  /**
    *
    * @return vrai s'il s'agit d'un départ de train, faux s'il s'agit d'un arrivé
    */
  def Depart():Boolean
  /**
    *
    * @return faux s'il s'agit d'un départ de train, vrai s'il s'agit d'un arrivé
    */
  def Arrive():Boolean



  /**
    * le traitement pricipal lancé pour chaque data source
    */

  def start() : Unit = {

    val sparkSession = SparkSession.builder.
      master(SPARK_MASTER)
      .appName(PPIV)
      .getOrCreate()
    import sparkSession.implicits._
    //read data from csv file
    val ds = sparkSession.read.option("header","true").option("inferSchema","true").csv("src/main/resources/sales.csv").as[TgaTgd]
    
    process(ds)
  }

  /**
    * Nettoyer la data
    * @param ds issu des fichiers sources TGA/TGD
    */
  def clean(ds: Dataset[TgaTgd]) :Unit= {

  }
  /**
    * Traitement des sources de données
    */
  def process(ds: Dataset[TgaTgd]): Unit =  {
    /**
      * convertir les date, nettoyer la data, filtrer la data, sauvegarde dans refinery
      */
    clean (ds)
    PersistTgaTgd.persisteTgaTgdCleandHive(ds)
    /**
      * Croiser la data avec le refernetiel et sauvegarder dans  un Gold
      */

    PersistTgaTgd.persisteTgaTgdGoldHive(ds)

  }


}



