package com.sncf.fab.ppiv.pipelineData.libPipeline

import java.text.SimpleDateFormat
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter

import com.sncf.fab.ppiv.utils.Conversion
import com.sncf.fab.ppiv.utils.Conversion.ParisTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * Created by ELFI03951 on 13/07/2017.
  */
object BusinessConversion {

  def getDateExtract(timestamp: Long) : String = {
    // Extraction de la date pour l'exemple
    val dernier_affichage = Conversion.unixTimestampToDateTime(timestamp)
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val date_extract = fmt.print(dernier_affichage)
    date_extract
  }

  def getMois(timestamp: Long) : String = {
    val mois = Conversion.unixTimestampToDateTime(timestamp).getMonthOfYear.toString
    mois
  }

  def getAnnee(timestamp: Long) : String = {
     val annee = Conversion.unixTimestampToDateTime(timestamp).getYear.toString
    annee
  }

  def getCreneau_horaire (timestamp: Long) : String = {
     val interval_depart_min = Conversion.unixTimestampToDateTime (timestamp).getHourOfDay.toString
     val interval_depart_max = (Conversion.unixTimestampToDateTime (timestamp).getHourOfDay +1).toString
     val interval_depart = "["+interval_depart_min +" - " +interval_depart_max+"]"
       interval_depart
  }

  def getNumberoftheday (timestamp:Long) : Int = {
    0
  }

  def getThreeFirstLettersOftheday (timestamp:Long) : String = {
    //TODO: Formatter: Mois  (en anglais et sur trois lettres)
    val dernier_affichage = Conversion.unixTimestampToDateTime (timestamp).monthOfYear().getAsShortText
      dernier_affichage
  }

  def getDelai_affichage_voie_sans_retard (timestamp:Long) : String = {
  val delai = Conversion.getHHmmss(timestamp)
    "H"+delai
  }

  def getDuree_temps_affichage(timestamp:Long): String = {
    //TODO segementation 0-5, 5-10 .. a parir de affichage durée 1 _minutes
    ""
  }
  def  getNbretard1(retard: Long) : Int= {
     if (retard != 0) 1
     else 0
  }

  def getDernier_retard_annonce_min (retard :Long) : Int= {
    (retard/60).toInt
  }

  def getTauxAffichage (duree_affichage: Long): Int ={
  //TODO  if duree_affichage is gretaer that 20 then 1 else 0
    0
  }
 def getAffichageRetard(timestamp : Long): String = {
   if (timestamp == 0) "0"
   else Conversion.unixTimestampToDateTime(timestamp).toString()
    }

  def getQuaiDevoiement ( devoiementInfo : Seq[String]) : String = {
    val quai1 = devoiementInfo(0)
    val quai2 = devoiementInfo(1)
    quai1 + "=>" + quai2
  }


  def getNbTotaldevoiement (devoiementInfo1 : Seq[String], devoiementInfo2 : Seq[String], devoiementInfo3 : Seq[String], devoiementInfo4 : Seq[String] ): Int = {
    val firstDevoiement  = devoiementInfo1  (2)
    val secondDevoiement = devoiementInfo2  (2)
    val thirdDevoiement  = devoiementInfo3  (2)
    val fourthDevoiement = devoiementInfo4  (2)

    val list_type_devoiement =List (firstDevoiement, secondDevoiement, thirdDevoiement, fourthDevoiement )

    val nb_devoiement = 4 - list_type_devoiement.count(_ == "NO DEV")
    nb_devoiement
  }

  def getNbDevoiement_affiche(devoiementInfo1 : Seq[String], devoiementInfo2 : Seq[String], devoiementInfo3 : Seq[String], devoiementInfo4 : Seq[String] ): Int = {
     val firstDevoiement  = devoiementInfo1  (2)
     val secondDevoiement = devoiementInfo2  (2)
     val thirdDevoiement  = devoiementInfo3  (2)
     val fourthDevoiement = devoiementInfo4  (2)

    val list_type_devoiement =List (firstDevoiement, secondDevoiement, thirdDevoiement, fourthDevoiement )

    list_type_devoiement.count(_ == "Affiche")
  }

  def getNvDevoiement_non_affiche(devoiementInfo1 : Seq[String], devoiementInfo2 : Seq[String], devoiementInfo3 : Seq[String], devoiementInfo4 : Seq[String] ): Int = {
    val firstDevoiement  = devoiementInfo1  (2)
    val secondDevoiement = devoiementInfo2  (2)
    val thirdDevoiement  = devoiementInfo3  (2)
    val fourthDevoiement = devoiementInfo4  (2)

    val list_type_devoiement =List (firstDevoiement, secondDevoiement, thirdDevoiement, fourthDevoiement )

    list_type_devoiement.count(_ == "Non_Affiche")
  }

  def getCracDevoiement (devoiementInfo1 : Seq[String], devoiementInfo2 : Seq[String], devoiementInfo3 : Seq[String], devoiementInfo4 : Seq[String] ): String = {

    val nbTotalDevoiement = getNbTotaldevoiement(devoiementInfo1, devoiementInfo2, devoiementInfo3, devoiementInfo4)

    if (nbTotalDevoiement != 0) {

      val nbDevoiementAffiche = getNbDevoiement_affiche(devoiementInfo1, devoiementInfo2, devoiementInfo3, devoiementInfo4)
      if (nbDevoiementAffiche != 0) "Devoiement affiche "
      else "Devoiement non affiche"
    }
    else {

      "No DEV"
    }

  }
    // TODO trouver pourquoi la conversion des float se fait aussi mal
  def getFloat(str : String): Float = {
      5
  }

}
