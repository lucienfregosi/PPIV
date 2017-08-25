package com.sncf.fab.ppiv.pipelineData.libPipeline

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import com.sncf.fab.ppiv.utils.Conversion
import com.sncf.fab.ppiv.utils.Conversion.ParisTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * Created by ELFI03951 on 13/07/2017.
  */
object BusinessConversion {

  //Function to Extract Date from Timestamp
  def getDateExtract(timestamp: Long): String = {
    val dernier_affichage = Conversion.unixTimestampToDateTime(timestamp)
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val date_extract = fmt.print(dernier_affichage)
    date_extract
  }

  //Function to Extract month from timestamp
  def getMois(timestamp: Long): String = {
    val mois = Conversion.unixTimestampToDateTime(timestamp).monthOfYear().getAsShortText()
    mois
  }

  // function to Extract Year from Timestamp
  def getAnnee(timestamp: Long): String = {
    val annee = Conversion.unixTimestampToDateTime(timestamp).getYear.toString
    annee
  }

  // function to extrat Creneau_horaire from timestamp
  def getCreneau_horaire(timestamp: Long): String = {
    val interval_depart_min = Conversion.unixTimestampToDateTime(timestamp).getHourOfDay
    val interval_depart_max = (Conversion.unixTimestampToDateTime(timestamp).getHourOfDay + 1)



    println("heure min: " + interval_depart_min)
    println("heure max: " + interval_depart_max)

    System.exit(0)

    // On souhaite le format 09 -> 10
    val interval_depart =  Conversion.HourFormat(interval_depart_min) + " -> " + Conversion.HourFormat(interval_depart_max)

    interval_depart
  }

  //function to get the number of the day  (Dimanche ==> 1)
  def getNumberoftheday(timestamp: Long): Int = {

    val cLocale = new Locale("en")
    val this_day= Conversion.unixTimestampToDateTime(timestamp).dayOfWeek().getAsShortText(cLocale)
    val mapDay_NumberOfDay = Map("Sun"-> 1,"Mon"->2, "Tue" -> 3, "Wed"->4, "Thu"->5, "Fri" ->6, "Sat" ->7 )

    mapDay_NumberOfDay(this_day)
  }

  //function to get the first three letter of the day
  def getThreeFirstLettersOftheday(timestamp: Long): String = {
    val cLocale = new Locale("en")
   val this_day = Conversion.unixTimestampToDateTime(timestamp).dayOfWeek().getAsShortText(cLocale)
    this_day
  }

  //fonction qui renvoie le delai d'affichage  de la dernière voie sans retard
  def getDelai_affichage_voie_sans_retard(timestamp: Long): String = {

    // On souhaite le format H(+ ou -)XXX avec XXX le nombre de minutes ou il est resté affiché
    if(timestamp < 0) "H-"+ TimeUnit.MILLISECONDS.toMinutes(timestamp.abs * 1000)
    else "H+"+ TimeUnit.MILLISECONDS.toMinutes(timestamp * 1000)
  }

  //fonction qui renvoie une segmentation de  la durée de l'affichage
  def getDuree_temps_affichage(timestamp: Long): String = {
    val duree_temps_affichage = TimeUnit.MILLISECONDS.toMinutes(timestamp * 1000)

    if (duree_temps_affichage <0) {
      "Voie affichée après ARR/DEP"
    }

    else if ( duree_temps_affichage >= 0 && duree_temps_affichage <= 5) {
      "[0-5]"
    }
    else if (duree_temps_affichage > 5 && duree_temps_affichage <= 10) {
      "[5-10]"
    }
    else if (duree_temps_affichage > 10 && duree_temps_affichage <= 20) {
      "[10-20]"
    }
    else if (duree_temps_affichage > 20 && duree_temps_affichage <= 30) {
      "[20-30]"
    }
    else if (duree_temps_affichage > 30 && duree_temps_affichage <= 40) {
      "[30-40]"
    }
    else if (duree_temps_affichage > 40 && duree_temps_affichage <= 50) {
      "[40-50]"
    }
    else if (duree_temps_affichage > 50 && duree_temps_affichage <= 60) {
      "[50-60]"
    }
    else if (duree_temps_affichage > 60 && duree_temps_affichage <= 120) {
      "[60-120]"
    }
    else {
      "Plus de 120 mn"
    }
  }

  //fonction qui renvoie 0 si il n'y a pas eu de retard, 1 s’il y a eu moins un retard
  def getNbretard1(retard: Long): Int = {
    if (retard != 0) 1
    else 0
  }

  //fonction qui renvoie 0 si il n'y a pas eu de retard, 1 s’il y a eu moins un retard
  def getNbretard2(retard: Long): Int = {
    if (retard != 0) 1
    else 0
  }

  //fonction qui renvoie le dernier retard annoncé en minutes
  def getDernier_retard_annonce_min(retard: Long): Int = {
    (retard / 60).toInt
  }

  //fonction  qui renvoie 1 si  duree d'affichage >=19 minutes
  def getTauxAffichage(duree_affichage: Long): Int = {
    //TODO  if duree_affichage is gretaer that 20 then 1 else 0
    val dureeAffichage = TimeUnit.MILLISECONDS.toMinutes(duree_affichage * 1000)
    if (dureeAffichage >= 19) 1
    else 0
  }

  //fonction qui convertit Affichage retard  (timestamp) à Affichage retard (DateTime) si il est différent de zero
  def getAffichageRetard(timestamp_affichage_retard: Long): String = {
    if (timestamp_affichage_retard == 0) null
    else Conversion.unixTimestampToDateTime(timestamp_affichage_retard).toString()
  }

  //fonction qui renvoie ( si devoiement )les quais du devoiement
  def getQuaiDevoiement(devoiementInfo: String): String = {
    try{
      val quai1 = devoiementInfo.split("-")(0)
      val quai2 = devoiementInfo.split("-")(1)
      quai1 + "=>" + quai2
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        ""
      }
    }
  }

  //fonction qui renvoie le nb total du devoiement
  def getNbTotaldevoiement(devoiementInfo1: String, devoiementInfo2: String, devoiementInfo3: String, devoiementInfo4: String): Int = {

    try{
      val list_devoiement = List(devoiementInfo1, devoiementInfo2, devoiementInfo3, devoiementInfo4)
      val nb_devoiement = list_devoiement.count(_ != "")
      nb_devoiement
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        0
      }
    }
  }

  //fonction qui renvoie le nb des devoiements affichés
  def getNbDevoiement_affiche(devoiementInfo1: String, devoiementInfo2: String, devoiementInfo3: String, devoiementInfo4: String): Int = {

    try{
      val list_devoiement = List(devoiementInfo1, devoiementInfo2, devoiementInfo3, devoiementInfo4)
      val list_devoiement_not_null = list_devoiement.filter(_ != null)
      val nbDevNonAffiche = list_devoiement_not_null.count(_.contains("Non_Affiche"))
      val nbTotalDev = getNbTotaldevoiement(devoiementInfo1, devoiementInfo2, devoiementInfo3, devoiementInfo4)
      nbTotalDev - nbDevNonAffiche

    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        0
      }
    }

  }

  //fonction qui renvoie le nb des devoiements non affichés
  def getNbDevoiement_non_affiche(devoiementInfo1: String, devoiementInfo2: String, devoiementInfo3: String, devoiementInfo4: String): Int = {

    try{
      val list_devoiement = List(devoiementInfo1, devoiementInfo2, devoiementInfo3, devoiementInfo4)
      val list_devoiement_not_null = list_devoiement.filter(_ != "")
      list_devoiement_not_null.count(_.contains("Non_Affiche"))
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        0
      }
    }

  }


  //fontion qui renvoie carac devoiement : si parmi les dévoiements il y en a eu au moins un affiché
  def getCaracDevoiement(devoiementInfo1: String, devoiementInfo2: String, devoiementInfo3: String, devoiementInfo4: String): String = {

    try{
      val nbTotalDevoiement = getNbTotaldevoiement(devoiementInfo1, devoiementInfo2, devoiementInfo3, devoiementInfo4)
      val nbDevoiementAffiche = getNbDevoiement_affiche(devoiementInfo1, devoiementInfo2, devoiementInfo3, devoiementInfo4)
      val nbDevoiementNonAffiche = getNbDevoiement_non_affiche(devoiementInfo1, devoiementInfo2, devoiementInfo3, devoiementInfo4)

      if (nbDevoiementAffiche > 0) "Devoiement affiche"
      else if(nbDevoiementNonAffiche > 0 ) "Devoiement non affiche"
      else ""

    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        ""
      }
    }

  }

  //fonction qui renvoie "H" concaténé au Délai où le train est resté affiché avant son départ réel (en minutes)
  def getDelai_affichage_voie_avec_retard(timestamp: Long): String = {
    "H+" + TimeUnit.MILLISECONDS.toMinutes(timestamp * 1000)
  }


  // fonction qui renvoie l'intervalle de temps du délai d'affichage de la voie avant le départ réel
  def getDuree_temps_affichage2(timestamp: Long): String = {
    val duree_temps_affichage2 = TimeUnit.MILLISECONDS.toMinutes(timestamp * 1000)

    if (duree_temps_affichage2 < 0) {
      "Voie affichée après ARR/DEP"
    }
    else if ( duree_temps_affichage2 >= 0 && duree_temps_affichage2 <= 5) {
      "[0-5]"
    }
    else if (duree_temps_affichage2 > 5 && duree_temps_affichage2 <= 10) {
      "[5-10]"
    }
    else if (duree_temps_affichage2 > 10 && duree_temps_affichage2 <= 20) {
      "[10-20]"
    }
    else if (duree_temps_affichage2 > 20 && duree_temps_affichage2 <= 30) {
      "[20-30]"
    }
    else if (duree_temps_affichage2 > 30 && duree_temps_affichage2 <= 40) {
      "[30-40]"
    }
    else if (duree_temps_affichage2 > 40 && duree_temps_affichage2 <= 50) {
      "[40-50]"
    }
    else if (duree_temps_affichage2 > 50 && duree_temps_affichage2 <= 60) {
      "[50-60]"
    }
    else if (duree_temps_affichage2 > 60 && duree_temps_affichage2 <= 120) {
      "[60-120]"
    }
    else {
      "Plus de 120 mn"
    }

  }

  // fonction qui renvoie "H" concaténe au Délai où le train est resté affiché avant son départ réel
  def getDelai_affichage_duree_retard(timestamp: Long): String = {
    if (timestamp == 0) null
    else "H+" + TimeUnit.MILLISECONDS.toMinutes(timestamp * 1000)

  }

  //fonction  qui renvoie 1 si  duree d'affichage >=29 minutes
  def geTaux_affichage_30(duree_affichage: Long): Int = {
    val dureeAffichage = TimeUnit.MILLISECONDS.toMinutes(duree_affichage * 1000)
    if (dureeAffichage >= 29) 1
    else 0
  }

  //fonction  qui renvoie 1 si  duree d'affichage >=44 minutes
  def geTaux_affichage_45(duree_affichage: Long): Int = {
    val dureeAffichage = TimeUnit.MILLISECONDS.toMinutes(duree_affichage * 1000)
    if (dureeAffichage >= 44) 1
    else 0
  }

  //fonction  qui renvoie 1 si  duree d'affichage >=14 minutes
  def geTaux_affichage_15(duree_affichage: Long): Int = {
    val dureeAffichage = TimeUnit.MILLISECONDS.toMinutes(duree_affichage * 1000)
    if (dureeAffichage >= 14) 1
    else 0
  }

  //fonction  qui extrait du devoiementInfo le type du devoiement
  def getTypeDevoiement(devoiementInfo: String) = {
    try{
      devoiementInfo.split("-")(2)
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        ""
      }
    }
  }

  //fonction qui renvoie la date d'affichade de l'etat du train en DateTime
  def getDateAffichageEtatTrain(timestamp: Long): String = {

    if (timestamp == 0) null
    else
      Conversion.unixTimestampToDateTime(timestamp).toString
  }


  // fonction qui  formate  Affichage duree retard en HHmmss si il est different de zero
  def getAffichage_duree_retard(timestamp: Long): String = {

    if (timestamp == 0) null
    else Conversion.getHHmmssFromMillis(timestamp)
  }

  //fonction quirenvoie Affichage duree retard en minutes
  def getAffichage_duree_retard_minutes(timestamp: Long): Int = {
    if (timestamp == 0) 0
    else TimeUnit.MILLISECONDS.toMinutes(timestamp * 1000).toInt
  }

  //fonction qui foramte Delai_affichage_etat_train_avant_depart_arrive en HHmmss
  def getDelai_affichage_etat_train_avant_depart_arrive(timestamp: Long): String = {
    if (timestamp == 0) null
    else Conversion.getHHmmssFromMillis(timestamp)

  }

}
