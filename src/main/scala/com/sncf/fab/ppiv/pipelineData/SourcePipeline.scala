package com.sncf.fab.ppiv.pipelineData


import com.sncf.fab.ppiv.business._
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.pipelineData.libPipeline.{BuildCycleOver, LoadData, Preprocess, ValidateData}
import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver.LOGGER
import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.Conversion
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext
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

  // Lancement du pipeline de traitement pour TGA et TGD
  def start(sc : SparkContext, sqlContext : SQLContext): Dataset[TgaTgdOutput] = {

    import sqlContext.implicits._

    // 1) Chargement des fichiers déjà parsé dans leur classe
    LOGGER.info("Chargement des fichiers et du référentiel")
    val dataTgaTgd                = LoadData.loadTgaTgd(sqlContext, getSource())
    val dataRefGares              = LoadData.loadReferentiel(sqlContext)

    // 2) Application du sparadrap sur les données au cause du Bug lié au passe nuit
    // On le conditionne a un flag dans app.conf car dans le futur Obier compte patcher le bug

    if (STICKING_PLASTER == true) LOGGER.info("Flag sparadrap activé, application de la correction")
    val dataTgaTgdBugFix = if (STICKING_PLASTER == true) Preprocess.applyStickingPlaster(dataTgaTgd, sqlContext) else dataTgaTgd

    // 3) Validation champ à champ
    LOGGER.info("Validation champ à champ")
    val (dataTgaTgdFielValidated, dataTgaTgdFielRejected)   = ValidateData.validateField(dataTgaTgdBugFix, sqlContext)


    // 4) Reconstitution des évènements pour chaque trajet
    // L'objectif de cette fonction est de renvoyer (cycleId | Array(TgaTgdInput) pour les cyclesId terminé
    // Et les TgaTgdInput de tout le cycle de vie du train (toute la journée + journée précédente pour les passe nuits)
    LOGGER.info("Reconstitution des cycles avec les évènements terminés")
    val cycleWithEventOver = BuildCycleOver.getCycleOver(dataTgaTgdFielValidated, sc, sqlContext, Panneau())



    // 5) Boucle sur les cycles finis
    val ivTgaTgdWithoutReferentiel = cycleWithEventOver.map{ x =>
      // Boucle sur chacun des cycles id terminés

      //val cycleId = x.getString(0)
      //val eventTgaTgd = x.getAs[Row(1)

      val cycleId = x.getString(0)
      val seq = x.getSeq[String](1)

      val seqTgaTgd = seq.map(x => {
        // Boucle sur les évènements pour pouvoir construire des Seq[TgaTgdInput)
        val split = x.toString.split(",",-1)
        TgaTgdInput(split(0), split(1).toLong, split(2), split(3), split(4), split(5), split(6), split(7), split(8), split(9).toLong, split(10), split(11))
      })


      // 6) Validation des cycles
      val isCycleValidated  = validateCycle(seqTgaTgd)
      if(isCycleValidated == false){
        println("invalidate cycle")
      }

      // 7) Nettoyage et mise en forme
      val dataTgaTgdCycleCleaned    = cleanCycle(seqTgaTgd)

      // 8) On sauvegarde un fichier par cycle dans refinery
      // TODO Voir ou G&C veulent qu'on charge leur données
      //saveCleanData(dataTgaTgdCycleCleaned)


      // 9) Calcul des différents règles de gestion.
      val premierAffichage = getPremierAffichage(dataTgaTgdCycleCleaned)
      val affichageDuree1  = getAffichageDuree1(dataTgaTgdCycleCleaned)
      val affichageDuree2  = getAffichageDuree2(dataTgaTgdCycleCleaned)

      //dataTgaTgdCycleCleaned

      // 10) Création d'une classe prenant toutes les règles de gestion (sans les conversions) à joindre au référentiel
      TgaTgdWithoutRef(cycleId,seqTgaTgd(0).gare,seqTgaTgd(0).ordes,seqTgaTgd(0).num,seqTgaTgd(0).`type`,seqTgaTgd(0).heure,seqTgaTgd(0).etat, premierAffichage, affichageDuree1, affichageDuree2)
    }

    // 10) Jointure avec le référentiel
    val dataTgaTgdWithReferentiel = joinReferentiel(ivTgaTgdWithoutReferentiel.toDS(), dataRefGares, sqlContext)

    // 12) Inscription dans la classe finale TgaTgdOutput avec conversion et formatage
    val dataTgaTgdOutput = getTgaTgdOutput(dataTgaTgdWithReferentiel, sqlContext)


    dataTgaTgdOutput

  }



  def validateCycle(dsTgaTgdSeq: Seq[TgaTgdInput]): Boolean = {

    // Validation des cycles. Un cycle doit comporter au moins une voie et tous ses évènements ne peuvent pas se passer x minutes après le départ du train
    // En entrée la liste des évènements pour un cycle id donné.
    // Décompte du nombre de lignes ou il y a une voie

    //  val cntVoieAffiche = dsTgaTgd.toDF().select("voie").filter($"voie".notEqual("")).filter($"voie".isNotNull).filter($"voie".notEqual("0")).count()
    val cntVoieAffiche = dsTgaTgdSeq.filter(x => (x.voie!= null ) && (x.voie!= "0")&& (x.voie!= "")).length

    // Compter le nombre d'évènements après le départ théroque + retard

    // val departThéorique = dsTgaTgd.toDF().select("heure").first().getAs[Long]("heure")
    val departThéorique = dsTgaTgdSeq(0).heure.toLong

    val retard = getCycleRetard(dsTgaTgdSeq)
    // 10 minutes : pour la marge d'erreur imposé par le métier. A convertir en secondes
    val margeErreur = 10 * 60
    val departReel = departThéorique + retard + margeErreur

    // val cntEventApresDepart = dsTgaTgd.toDF().filter($"maj".gt(departReel)).count()
    val cntEventApresDepart = dsTgaTgdSeq.filter(x=>( x.maj > departReel)).length

    // Si le compte de voie est différent de 0 ou le compte des évènement après la date est égale a la somme des event (= tous les évènements postérieurs à la date de départ du train
    if(cntVoieAffiche != 0 && cntEventApresDepart != dsTgaTgdSeq.length ){
      true
    }
    else{
      false
    }
  }



  def cleanCycle(seqTgaTgd: Seq[TgaTgdInput]): Seq[TgaTgdInput] = {
    // Nettoyage, mise en forme des lignes, conversion des heures etc ..
    seqTgaTgd
  }

  def saveCleanData(seqTgaTgd: Seq[TgaTgdInput]): Unit = {
    // Sauvegarde des données pour que G&C ait un historique d'Obier exploitable
    None
  }

  def joinReferentiel(dsTgaTgd: Dataset[TgaTgdWithoutRef],  refGares : Dataset[ReferentielGare], sqlContext : SQLContext): DataFrame = {
    // Jointure avec le référentiel pour enrichir les lignes
    import sqlContext.implicits._

    val joinedData = dsTgaTgd.toDF().join(refGares.toDF(), dsTgaTgd.toDF().col("gare") === refGares.toDF().col("TVS"))
    joinedData
  }

  def getTgaTgdOutput(dfTgaTgd: DataFrame, sqlContext : SQLContext) : Dataset[TgaTgdOutput] = {
    import sqlContext.implicits._


    val affichageFinal =  dfTgaTgd.map(row => TgaTgdOutput(
      row.getString(11),
      row.getString(22),
      row.getString(15),
      row.getString(13),
      row.getString(25),
      row.getString(26),
      row.getString(0),
      row.getString(3),
      row.getString(4),
      row.getString(2),
      Panneau(),
      Conversion.unixTimestampToDateTime(row.getLong(5)).toString,
      Conversion.unixTimestampToDateTime(row.getLong(7)).toString,
      Conversion.unixTimestampToDateTime(row.getLong(8)).toString,
      Conversion.unixTimestampToDateTime(row.getLong(9)).toString
    ))

    affichageFinal.toDS().as[TgaTgdOutput]

  }



  /********************** Fonction de calcul des régles métiers qui prennent un data set input ********************/



  def getCycleRetard(dsTgaTgd: Seq[TgaTgdInput]) : Long = {
    // Filtre des retard et tri selon la date d'èvènement pour que le retard soit en dernier

    val seqFiltered = dsTgaTgd.filter(x => (x.retard !=null) && (x.retard !="") && (x.retard !="0"))
    //val dsFiltered = dsTgaTgd.toDF().orderBy($"maj".asc).filter($"retard".isNotNull).filter($"retard".notEqual("")).filter($"retard".notEqual("0"))

    // Si 0 retard on renvoie la valeur 0
    if(seqFiltered.isEmpty){
      0
    } else {
      // On trie dans le sens décroissant pour prendre le dernier retard
      //  val minuteRetard = dsFiltered.orderBy($"maj".desc).first().getString(11).toLong

      seqFiltered.foreach(println)


      val minuteRetardFilred = seqFiltered.sortBy(x=>x.maj)
      val  minuteRetard =  minuteRetardFilred(0).retard.toLong
      // Multipliation par 60 pour renvoyer un résultat en secondes
      minuteRetard * 60
    }
  }


  // Fonction qui renvoie la date de premier affichage de la voie pour un cycle donné
  def getPremierAffichage(seqTgaTgd: Seq[TgaTgdInput]) : Long = {

      // Récupération de la date de premier affichage. On cherche le moment ou la bonne voie a été affiché pour la première fois


    val dsVoieGrouped = seqTgaTgd.sortBy(_.maj ).reverse.filter(x => x.voie != null && x.voie != "" &&  x.voie   != ("0")).groupBy(_.voie).map{ case(_,group)=> ( group.map(_.maj).min)}
    if (dsVoieGrouped.size == 0){
      0
    }
    else{
      dsVoieGrouped.head
    }

  }

  // Fonction qui renvoie le temps durant lequel le train est resté affiché. On retourne un timestamp
  def getAffichageDuree1(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {

    //val departTheorique = dsTgaTgd.toDF().first().getLong(9)
    val departTheorique = dsTgaTgdSeq(0).heure.toLong
    //println("depart théroique" + departTheorique)


    val premierAffichage = getPremierAffichage(dsTgaTgdSeq)
    //println("premier affichage" + premierAffichage)

    departTheorique - premierAffichage
  }


  // Fonction qui renvoie le temps durant le quel le train est resté affiché retard compris. On retourne un timestamp
  def getAffichageDuree2(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {
    val affichageDuree1 = getAffichageDuree1(dsTgaTgdSeq)
    val retard = getCycleRetard(dsTgaTgdSeq)

    affichageDuree1 + retard
  }

}



