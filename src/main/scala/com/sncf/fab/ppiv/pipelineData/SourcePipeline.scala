package com.sncf.fab.ppiv.pipelineData

import java.util.Date

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.business.{ReferentielGare, TgaTgdInput, TgaTgdOutput, TgaTgdTransitionnal}
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.utils.AppConf._
import org.apache.spark.SparkConf
import com.sncf.fab.ppiv.utils.Conversion
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import com.sncf.fab.ppiv.persistence.{PersistElastic, PersistHdfs, PersistHive, PersistLocal}
import com.sncf.fab.ppiv.utils.Conversion.ParisTimeZone
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




    // 2) Application du sparadrap sur les données au cause du Bug lié au passe nuit. Flag pour pouvoir le désactiver
    val dataTgaTgdBugFix = if (STICKING_PLASTER == true) applyStickingPlaster(dataTgaTgd, sqlContext) else dataTgaTgd

    // 3) Validation champ à champ
    println("CNT initial" + dataTgaTgdBugFix.count())
    val dataTgaTgdFielValidated   = validateField(dataTgaTgdBugFix, sqlContext)


    // 4) Sélection des lignes dont les cycles sont terminé et enrichissement dans les fichiers horaires précédents
    // TODO : Pour les passes nuits aller chercher dans les fichiers de la veille aussi
    // Le groupement des cycles et cette étape est commune
    // Ressortir un tableau avec tous les cycles id a traiter et les traiter dans un boucle
    val dataTgaTgdCycleOver       = filterCycleOver(dataTgaTgdFielValidated, sqlContext)


    // 5) Pour un cycle ID donné on récupère tous ses évènements dans les fichiers précédents
    val dataTgaTgdGrouped         = getEventCycleId(dataTgaTgdCycleOver, sqlContext)

    // Fonction pour aller cherche les évènements des fichiers horaires


    // 6) Validation des cycles
    val dataTgaTgdCycleValidated  = validateCycle(dataTgaTgdFielValidated, sqlContext)

    // 7) Nettoyage et mise en forme
    val dataTgaTgdCycleCleaned    = cleanCycle(dataTgaTgdGrouped, sqlContext)

    // 8) Sauvegarde des données propres la ou G&C le souhaite
    saveCleanData(dataTgaTgdCycleCleaned, sqlContext)

    // 9) Calcul des différents règles de gestion.
    val dataTgaTgdCycleKPI        = computeOutputFields(dataTgaTgdCycleCleaned, sqlContext)

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
    // Voir comment traiter les rejets ..
    val currentTimestamp = DateTime.now(DateTimeZone.UTC).getMillis() / 1000

    // Valid
    //dsTgaTgd.show()
    val dsTgaTgdValidatedFields = dsTgaTgd
      .filter(_.gare matches "^[A-Z]{3}$" )
      .filter(_.maj <= currentTimestamp)
      .filter(_.train matches  "^[0-2]{0,1}[0-9]$")
     // .filter(_.ordes matches "(^[A-Z|\\s]*$)")
      //.filter(_.`type` matches "^([A-Z]+$)") // Il en enlève 100
     // .filter(x => ((x.attribut_voie matches "I") && (x.voie matches "^(?:[0-9]|[A-Z]|$)$" )) ||((x.attribut_voie matches "\\s||$") && (x.voie matches "^(?:[0-9]|[A-Z])$" )))
     // .filter(_.etat matches "^(?:(IND)|(SUP)|(ARR)|$|(\\s))$")
     // .filter(_.retard matches  "^(([0-9]{4})|([0-9]{2})|$|\\s)$")

    // Rejected
   val dsTgaTgdRejectedFields = dsTgaTgd.filter(x => (x.gare matches("^(?!([A-Z]{3}))$")) || (x.maj > currentTimestamp)
     ||  (x.train matches  "^(?!([0-2]{0,1}[0-9]))$")
     ||(x.train matches  "^(?!([0-2]{0,1}[0-9]))$")
     //||  (x.ordes matches "^(?!([A-Z|\\s]*))$")
    // ||  (x.`type` matches "^(?!([A-Z]+))$")
     //||((x.attribut_voie matches "(?!(^I$))") || (x.voie matches "^(?!(?:[0-9]|[A-Z]|$))$" )) &&((x.attribut_voie matches "(?!(\\s||$))") || (x.voie matches "^(?!(?:[0-9]|[A-Z]))$" ))
        //|| (x.etat matches "^(?!(?:(IND)|(SUP)|(ARR)|$|\\s))$")
     //|| (x.retard matches  "^(?!(?:[0-9]{2}|[0-9]{4}|$|\\s))$")
   )



    println("Count accepted  " + dsTgaTgdValidatedFields.count())
    println("Count rejected  " + dsTgaTgdRejectedFields.count())

     // Sauvegarde des rejets
    //PersistElastic.persisteTgaTgdParsedIntoEs(dsTgaTgdRejectedFields,"ppiv/rejectedField")

    dsTgaTgdValidatedFields

  }


  def filterCycleOver(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext): Dataset[TgaTgdTransitionnal] = {
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

  def getEventCycleId(dsTgaTgd: Dataset[TgaTgdTransitionnal], sqlContext : SQLContext): Dataset[TgaTgdTransitionnal] = {
    import sqlContext.implicits._
    // Pour un cycle id renvoyer la liste des évènements trouvé dans les fichiers horaires de la journée
    dsTgaTgd
  }

  def validateCycle(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext): Boolean = {
    import sqlContext.implicits._
    // Validation des cycles. Un cycle doit comporter au moins une voie et tous ses évènements ne peuvent pas se passer x minutes après le départ du train
    // En entrée la liste des évènements pour un cycle id donné.

    // Décompte du nombre de lignes ou il y a une voie
    //val cntVoieAffiche = dsTgaTgd.toDF().select("voie").filter($"voie".isNotNull.notEqual("").notEqual("0")).count()
    val cntVoieAffiche = dsTgaTgd.toDF().select("voie").filter($"voie".notEqual(""))
      .filter($"voie".isNotNull).filter($"voie".notEqual("0")).count()

    // Compter le nombre d'évènements après le départ théroque + retard
    val departThéorique = dsTgaTgd.toDF().select("heure").first().getAs[Long]("heure")
    val retard = getCycleRetard(dsTgaTgd, sqlContext)
    // 10 minutes : pour la marge d'erreur imposé par le métier. A convertir en secondes
    val margeErreur = 10 * 60
    val departReel = departThéorique + retard + margeErreur

    val cntEventApresDepart = dsTgaTgd.toDF().filter($"maj".gt(departReel)).count()

    // Si le compte de voie est différent de 0 ou le compte des évènement après la date est égale a la somme des event (= tous les évènements postérieurs à la date de départ du train
    if(cntVoieAffiche != 0 && cntEventApresDepart != dsTgaTgd.count()){
      true
    }
    else{
      false
    }
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

  def computeOutputFields(dsTgaTgd: Dataset[TgaTgdTransitionnal], sqlContext : SQLContext): Dataset[TgaTgdTransitionnal] = {
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



  /********************** Fonction de calcul des régles métiers qui prennent un data set input ********************/

  // TODO : Faire un test pour cette fonction
  def getCycleRetard(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext) : Long = {
    import sqlContext.implicits._
    // Filtre des retard et tri selon la date d'èvènement pour que le retard soit en dernier
    val dsFiltered = dsTgaTgd.toDF().orderBy($"maj".asc)
      .filter($"retard".isNotNull)
      .filter($"retard".notEqual(""))
      .filter($"retard".notEqual("0"))

    // Si 0 retard on renvoie la valeur 0
    if(dsFiltered.count() == 0){
      0
    } else {
      // On trie dans le sens décroissant pour prendre le dernier retard
      val minuteRetard = dsFiltered.orderBy($"maj".desc).first().getString(11).toLong

      // Multipliation par 60 pour renvoyer un résultat en secondes
      minuteRetard * 60
    }
  }

  // Fonction qui renvoie la date de premier affichage de la voie pour un cycle donné
  def getPremierAffichage(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext) : Long = {
    import sqlContext.implicits._
    //
    // Récupération de la date de premier affichage. On cherche le moment ou la bonne voie a été affiché pour la première fois

    // Filtre des lignes qui ne contiennent pas de voie. Puis group sur les vois et pour chaque voie on sélectionne le min de maj (le moment ou ell est affichée)
    val dsVoieGrouped = dsTgaTgd.toDF().orderBy(asc("maj")).filter($"voie".isNotNull).filter($"voie".notEqual("")).filter($"voie".notEqual("0"))
      .groupBy("voie").agg(min($"maj") as 'premierAffichageParVoie)

    //dsVoieGrouped.show()

    // Sélection de la dernière des voie apparaissant et de son timestamp correspondant au premier affichage
    dsVoieGrouped.orderBy($"premierAffichageParVoie".desc).first().getLong(1)

  }

  // Fonction qui renvoie le temps durant lequel le train est resté affiché. On retourne un timestamp
  def getAffichageDuree1(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext) : Long = {
    import sqlContext.implicits._

    val departTheorique = dsTgaTgd.toDF().first().getLong(9)
    //println("depart théroique" + departTheorique)


    val premierAffichage = getPremierAffichage(dsTgaTgd, sqlContext)
    //println("premier affichage" + premierAffichage)

    departTheorique - premierAffichage
  }

  // Fonction qui renvoie le temps durant le quel le train est resté affiché retard compris. On retourne un timestamp
  def getAffichageDuree2(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext) : Long = {
    val affichageDuree1 = getAffichageDuree1(dsTgaTgd, sqlContext)
    val retard = getCycleRetard(dsTgaTgd, sqlContext)

    affichageDuree1 + retard
  }
}



