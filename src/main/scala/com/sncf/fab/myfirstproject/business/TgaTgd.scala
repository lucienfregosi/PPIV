package com.sncf.fab.myfirstproject.business
import org.joda.time.DateTime

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
/** Class for final result, to check if train has been late and if it has changed the way*/
case class TgaTgd(nomGare:String, agence:String, date_affichage:DateTime, uic:Int, typeTrain:String, typePanneau:String, origineDestination:String, depart:Boolean, arrive:Boolean, retard:Boolean, devoiement:Boolean)
