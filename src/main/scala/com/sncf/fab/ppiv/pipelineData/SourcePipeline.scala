package com.sncf.fab.ppiv.pipelineData

import java.util.Date

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.business._
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.utils.AppConf._
import org.apache.spark.SparkConf
import com.sncf.fab.ppiv.utils.Conversion
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import com.sncf.fab.ppiv.persistence.{PersistElastic, PersistHdfs, PersistHive, PersistLocal}
import com.sncf.fab.ppiv.utils.Conversion.ParisTimeZone
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



  /**
    * le traitement principal lancé pour chaque data source
    */

  def start(outputs: Array[String], sc : SparkContext, sqlContext : SQLContext): Dataset[TgaTgdOutput] = {

    import sqlContext.implicits._
    // Début du Pipeline

    // 1) Chargement des fichiers déjà parsé dans leur classe
    val dataTgaTgd                = loadTgaTgd(sqlContext)
    val dataRefGares              = loadReferentiel(sqlContext)




    // 2) Application du sparadrap sur les données au cause du Bug lié au passe nuit. Flag pour pouvoir le désactiver
    val dataTgaTgdBugFix = if (STICKING_PLASTER == true) applyStickingPlaster(dataTgaTgd, sqlContext) else dataTgaTgd

    // 3) Validation champ à champ
    println("CNT initial" + dataTgaTgd.count())
    val dataTgaTgdFielValidated   = validateField(dataTgaTgdBugFix, sqlContext)


    // 4) Reconstitution des évènements pour chaque trajet
      // Récupération de tous les cycles d'un fichier horaire et sélection des terminés
    val cycleIdList       = buildCycles(dataTgaTgdFielValidated, sqlContext)
    val cycleIdListOver   = filterCycleOver(cycleIdList, sqlContext)
    val tgaTgdCycleOver   = getEventCycleId(cycleIdListOver, sqlContext, sc)



    // 5) Boucle sur les cycles finis
    val ivTgaTgdWithoutReferentiel = tgaTgdCycleOver.select("event").map{ x =>
      // Boucle sur chacun des cycles id terminés

      //val cycleId = x.getString(0)
      //val eventTgaTgd = x.getAs[Row(1)

      val seq = x.getSeq(0)

      val seqTgaTgd = seq.map(x => {
        // Boucle sur les évènements pour pouvoir construire des Seq[TgaTgdInput)
        val split = x.toString.split(",")
        TgaTgdInput(split(0), split(1).toLong, split(2), split(3), split(4), split(5), split(6), split(7), split(8), split(9).toLong, split(10), split(11))
      })

      seqTgaTgd.length



      // 6) Validation des cycles
      //val isCycleValidated  = validateCycle(seqTgaTgd)
      //if(isCycleValidated == false){println("Faire sortir de la boucle")}

      // 7) Nettoyage et mise en forme
      //val dataTgaTgdCycleCleaned    = cleanCycle(seqTgaTgd)
      //dataTgaTgdCycleCleaned

      // 8) On sauvegarde un fichier par cycle dans refinery
      // TODO Voir ou G&C veulent qu'on charge leur données
      //saveCleanData(dataTgaTgdCycleCleaned)


      // 9) Calcul des différents règles de gestion.
      //val premierAffichage = getPremierAffichage(dataTgaTgdCycleCleaned)
      //val affichageDuree1  = getAffichageDuree1(dataTgaTgdCycleCleaned)
      //val affichageDuree2  = getAffichageDuree2(dataTgaTgdCycleCleaned)

      //dataTgaTgdCycleCleaned

      // 10) Création d'une classe prenant toutes les règles de gestion (sans les conversions) à joindre au référentiel
      //TgaTgdWithoutRef("t",seqTgaTgd(0).gare,seqTgaTgd(0).ordes,seqTgaTgd(0).num,seqTgaTgd(0).`type`,seqTgaTgd(0).heure,seqTgaTgd(0).etat, premierAffichage, affichageDuree1, affichageDuree2)

    }


    ivTgaTgdWithoutReferentiel.take(5).foreach(println)


    System.exit(0)

    /*
    // 10) Jointure avec le référentiel
    val dataTgaTgdWithReferentiel = joinReferentiel(ivTgaTgdWithoutReferentiel, dataRefGares, sqlContext)

    // 11) Conversion diverses, formatage de la sortie
    val dataClean = formatData(dataTgaTgdWithReferentiel, sqlContext)

    // 12) Inscription dans la classe finale TgaTgdOutput
    val dataTgaTgdOutput = getTgaTgdOutput(dataClean, sqlContext)


    dataTgaTgdOutput.show()



    dataTgaTgdOutput
    */
    null
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


  def applyStickingPlaster(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext): Dataset[TgaTgdInput] = {
    import sqlContext.implicits._
    // Application du sparadrap
    // Pour les trains passe nuit, affiché entre après 18h et partant le lendemain il y a un bug connu et identifié dans OBIER
    // Les évènements de 18à 24h seront a la date N+1. Il faut donc leur retrancher un jour pour la cohérence

    // Si maj > 18 && heure < 12 on retranche un jour a la date de maj

    val dsTgaTgdWithStickingPlaster = dsTgaTgd.map{
      row =>
        val hourMaj    = new DateTime(row.maj).toDateTime.toString("hh").toInt
        val hourHeure  = new DateTime(row.heure).toDateTime.toString("hh").toInt
        val newMaj = if(hourMaj > 18 && hourHeure < 12){
          // On retranche un jour
          new DateTime(row.maj).plusDays(-1).getMillis / 1000
        } else row.maj
        TgaTgdInput(row.gare, newMaj, row.train, row.ordes, row.num,row.`type`, row.picto, row.attribut_voie, row.voie, row.heure, row.etat, row.retard)
    }
    dsTgaTgdWithStickingPlaster
  }

  def validateField(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext): Dataset[TgaTgdInput] = {
    import sqlContext.implicits._
    // Validation de chaque champ avec les contraintes définies dans le dictionnaire de données
    val currentTimestamp = DateTime.now(DateTimeZone.UTC).getMillis() / 1000

    // Valid

    //dsTgaTgd.show()
    val dsTgaTgdValidatedFields = dsTgaTgd
      .filter(_.gare matches "^[A-Z]{3}$" )
      .filter(_.maj <= currentTimestamp)
      .filter(_.train matches  "^[0-2]{0,1}[0-9]$")
      .filter(_.`type` matches "^([A-Z]+)$")
      .filter(_.attribut_voie matches "I|$")
      .filter(_.attribut_voie matches "^(?:[0-9]|[A-Z]|$)$")
      .filter(_.etat matches "^(?:(IND)|(SUP)|(ARR)|$|(\\s))$")
      .filter(_.retard matches  "^(([0-9]{4})|([0-9]{2})|$|\\s)$")

    // Rejected
    val dsTgaTgdRejectedFields = dsTgaTgd.filter(x => (x.gare matches("(?!(^[A-Z]{3})$)")) || (x.maj > currentTimestamp)
      ||  (x.train matches  "(?!(^[0-2]{0,1}[0-9]$))")
      ||  (x.`type` matches "(?!(^[A-Z]+$))")
      ||  (x.attribut_voie matches "!(I|$)")
      ||  (x.voie matches "(?!(^(?:[0-9]|[A-Z]|$)$))")
      || (x.etat matches "(?!(^(?:(IND)|(SUP)|(ARR)|$|\\s)$))")
      || (x.retard matches  "(?!(^(?:[0-9]{2}|[0-9]{4}|$|\\s)$))")
    )
    // Sauvegarde des rejets
    //PersistElastic.persisteTgaTgdParsedIntoEs(dsTgaTgdRejectedFields,"ppiv/rejectedField")
    dsTgaTgdValidatedFields

  }


  def buildCycles(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext) : Dataset[TgaTgdCycle] = {
    import sqlContext.implicits._
    dsTgaTgd.toDF().registerTempTable("dataTgaTgd")
    val dataTgaTgCycles = sqlContext.sql("SELECT concat(gare,'" + Panneau() + "',num,heure) as cycle_id, first(heure) as heure," +
      " last(retard) as retard" +
      " from dataTgaTgd group by concat(gare, '" + Panneau() + "',num,heure)")
      .withColumn("heure", 'heure.cast(LongType))
      .as[TgaTgdCycle]

    //dataTgaTgCycles.show()
    dataTgaTgCycles
  }

  def filterCycleOver(dsTgaTgdCycles : Dataset[TgaTgdCycle], sqlContext : SQLContext):  Dataset[TgaTgdCycle]= {
    import sqlContext.implicits._
    val horaireMax = Conversion.nowToDateTime().plusHours(-1)

    val dataTgaTgdCycleOver = dsTgaTgdCycles .filter(x =>
      ( (new  DateTime(x.heure).plusMinutes(x.retard.toInt)) isBefore(horaireMax) ))

    dsTgaTgdCycles

  }

  def getEventCycleId(dsTgaTgdCyclesOver : Dataset[TgaTgdCycle], sqlContext : SQLContext, sc : SparkContext): DataFrame = {

    val hiveContext = new HiveContext(sc)
    import sqlContext.implicits._

    // A partir de la liste des cycles finis, reconstitution d'un DS de la forme cycleId| Seq(gare, maj, ...)
    // TODO : Charger les TGA TGD de la journée
    val tgaTgdInputAllDay = loadTgaTgd(sqlContext).toDF().withColumn("cycle_id2",concat(col("gare"),lit(Panneau()),col("num"), col("heure")))

    // On joint les deux avec un left join pour garder seulement les cycles terminés
    val dfJoin = dsTgaTgdCyclesOver.toDF().select("cycle_id").join(tgaTgdInputAllDay, $"cycle_id" === $"cycle_id2","LeftOuter")


    val hiveDataframe = hiveContext.createDataFrame(dfJoin.rdd, dfJoin.schema)

   // On concatène les colonnes pour pouvoir manipuler plus facilement la colonne dans le group byu
    // On reconstruiera un TgaTgdInput plus tard
    val dfGroupByCycleOver = hiveDataframe.drop("cycle_id2").distinct().select($"cycle_id", concat($"gare",lit(","),$"maj",lit(","),$"train",lit(","),$"ordes",lit(","),$"num",lit(","),$"type",lit(","),$"picto",lit(","),$"attribut_voie",lit(","),$"voie",lit(","),$"heure",lit(","),$"etat",lit(","),$"retard") as "event")
        .groupBy("cycle_id").agg(
            collect_list($"event") as "event"
        )


    dfGroupByCycleOver
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

  def formatData(dfTgaTgd: DataFrame, sqlContext : SQLContext) : DataFrame = {
    dfTgaTgd
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
    dsVoieGrouped.head
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



