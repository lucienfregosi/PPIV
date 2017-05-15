package com.sncf.fab.myfirstproject.business

import java.sql.Date

/**
  * Created by simoh-labdoui on 11/05/2017.
  * Table issue des fichiers TGA/TGD filtrer et nettoyée (Avec formattage des champs)
  */

//case class TgaTgdParsed(nomGare:String, agence:String, date_affichage:DateTime, uic:Int, typeTrain:String, typePanneau:String, origineDestination:String, depart:Boolean, arrive:Boolean, retard:Boolean, devoiement:Boolean)
//case class TgaTgdParsed(gare:String,agence:String,numTrain: Int,dateTrain: Date,typeTrain:String,dateHeure1erAffichageVoie:Date,delaiAffichageVoieHorsRetard:Int,delaiAffichageVoieRetardCompris:String,instantAffichageHorsRetard:String,instantAffichageRetardCompris:String,dernierRetardAffiche:String,nbrRetardsConfirmes:String,nbDevoiementsTotal:String,nbDevoiementsAffiches:String,nbDevoiementsNonAffiches:String )
case class TgaTgdParsed(gare:String, maj:Long, train:String,
                        ordes:String, num:String, `type`:String,
                        picto:String, attribut_voie:String, voie:String,
                        heure:Long, etat:String, retard:String)

