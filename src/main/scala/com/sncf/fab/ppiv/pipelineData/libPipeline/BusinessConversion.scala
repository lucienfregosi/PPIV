package com.sncf.fab.ppiv.pipelineData.libPipeline

import java.text.SimpleDateFormat
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
    val dureeAffichage = TimeUnit.MILLISECONDS.toMinutes(duree_affichage * 1000 )
    if (dureeAffichage>=19) 1
    else 0
  }

 def getAffichageRetard(timestamp_affichage_retard : Long): String = {
   if (timestamp_affichage_retard == 0) null
   else Conversion.unixTimestampToDateTime(timestamp_affichage_retard).toString()
    }

  def getQuaiDevoiement ( devoiementInfo : String) : String = {

    if (devoiementInfo!= null) {
      val quai1 = devoiementInfo.split("-")(0)
      val quai2 = devoiementInfo.split("-")(1)
      quai1 + "=>" + quai2
    }
    else
      null
  }


  def getNbTotaldevoiement (devoiementInfo1 : String, devoiementInfo2 : String, devoiementInfo3 : String, devoiementInfo4 : String ): Int = {

    val list_devoiement = List(devoiementInfo1,devoiementInfo2, devoiementInfo3, devoiementInfo4)

    val nb_devoiement = list_devoiement.count(_!=null)

    /*
    val firstDevoiement  = devoiementInfo1.split("-")  (2)
    val secondDevoiement = devoiementInfo2.split("-")  (2)
    val thirdDevoiement  = devoiementInfo3.split("-")  (2)
    val fourthDevoiement = devoiementInfo4.split("-")  (2)

    val list_type_devoiement =List (firstDevoiement, secondDevoiement, thirdDevoiement, fourthDevoiement )

    val nb_devoiement = 4 - list_type_devoiement.count(_ == "NO DEV")
    */
    nb_devoiement
  }

  def getNbDevoiement_affiche(devoiementInfo1 :String, devoiementInfo2 : String, devoiementInfo3 : String, devoiementInfo4 : String ): Int = {
    val list_devoiement = List(devoiementInfo1,devoiementInfo2, devoiementInfo3, devoiementInfo4)
    val list_devoiement_not_null = list_devoiement.filter(_!=null)
    if (list_devoiement_not_null.length > 0) list_devoiement_not_null.count(_.contains("Affiche"))
    else 0

    /*
    val firstDevoiement  = devoiementInfo1.split("-")  (2)
     val secondDevoiement = devoiementInfo2.split("-")  (2)
     val thirdDevoiement  = devoiementInfo3.split("-")  (2)
     val fourthDevoiement = devoiementInfo4.split("-")  (2)

    val list_type_devoiement =List (firstDevoiement, secondDevoiement, thirdDevoiement, fourthDevoiement )

    list_type_devoiement.count(_ == "Affiche")

    */
  }

  def getNvDevoiement_non_affiche(devoiementInfo1 : String, devoiementInfo2 : String, devoiementInfo3 : String, devoiementInfo4 : String ): Int = {
    val list_devoiement = List(devoiementInfo1,devoiementInfo2, devoiementInfo3, devoiementInfo4)
    val list_devoiement_not_null = list_devoiement.filter(_!=null)
    if (list_devoiement_not_null.length > 0) list_devoiement_not_null.count(_.contains("Non_Affiche"))
    else 0

   /*
    val firstDevoiement  = devoiementInfo1.split("-")  (2)

    val secondDevoiement = devoiementInfo2.split("-")  (2)
    val thirdDevoiement  = devoiementInfo3.split("-")  (2)
    val fourthDevoiement = devoiementInfo4.split("-")  (2)

    val list_type_devoiement =List (firstDevoiement, secondDevoiement, thirdDevoiement, fourthDevoiement )

    list_type_devoiement.count(_ == "Non_Affiche")

    */
  }

  def getCracDevoiement (devoiementInfo1 : String, devoiementInfo2 : String, devoiementInfo3 :String, devoiementInfo4 : String ): String = {

    val nbTotalDevoiement = getNbTotaldevoiement(devoiementInfo1, devoiementInfo2, devoiementInfo3, devoiementInfo4)

    if (nbTotalDevoiement != 0) {

      val nbDevoiementAffiche = getNbDevoiement_affiche(devoiementInfo1, devoiementInfo2, devoiementInfo3, devoiementInfo4)
      if (nbDevoiementAffiche != 0) "Devoiement affiche "
      else "Devoiement non affiche"
    }
    else {

     null
    }

  }

  def getDelai_affichage_voie_avec_retard (timestamp : Long) : String = {
    "H-"+Conversion.getHHmmssFromMillis(timestamp)
  }

  def getDuree_temps_affichage2 (timestamp : Long): String = {
    val duree_temps_affichage2 = TimeUnit.MILLISECONDS.toMinutes(timestamp * 1000 )


    if (duree_temps_affichage2 <= 5) {
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
    else {
      "Plus de 60 mn"
    }

  }

  def getDelai_affichage_duree_retard (timestamp:Long) : String = {

    "H-"+Conversion.getHHmmssFromMillis(timestamp)
  }

  def geTaux_affichage_30 (duree_affichage : Long) : Int = {
    val dureeAffichage = TimeUnit.MILLISECONDS.toMinutes(duree_affichage * 1000 )
    if (dureeAffichage>=29) 1
    else 0
  }

  def geTaux_affichage_45 (duree_affichage : Long) : Int = {
    val dureeAffichage = TimeUnit.MILLISECONDS.toMinutes(duree_affichage * 1000 )
    if (dureeAffichage>=44) 1
    else 0
  }

  def geTaux_affichage_15 (duree_affichage : Long) : Int = {
    val dureeAffichage = TimeUnit.MILLISECONDS.toMinutes(duree_affichage * 1000 )
    if (dureeAffichage>=14) 1
    else 0
  }

  def getTypeDevoiement (devoiementInfo : String) = {
   if (devoiementInfo != null )
    devoiementInfo.split("-")(2)
   else null
  }
    // TODO trouver pourquoi la conversion des float se fait aussi mal
  def getFloat(str : String): Float = {
      5
  }

}
