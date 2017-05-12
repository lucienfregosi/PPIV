package com.sncf.fab.myfirstproject.business
import org.joda.time.DateTime

/**
  * Created by simoh-labdoui on 11/05/2017.
  * Table issue des fichiers TGA/TGD filtrer et nettoyée (Avec formattage des champs)
  */

case class TgaTgdParsed(nomGare:String, agence:String, date_affichage:DateTime, uic:Int, typeTrain:String, typePanneau:String, origineDestination:String, depart:Boolean, arrive:Boolean, retard:Boolean, devoiement:Boolean)
