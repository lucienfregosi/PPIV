package com.sncf.fab.ppiv.business

import java.sql.Date

/**
  * Created by simoh-labdoui on 11/05/2017.
  * Table issue des fichiers TGA/TGD filtrer et nettoyée (Avec formattage des champs)
  */

case class TgaTgdInput(gare:String, maj:Long, train:String,
                       ordes:String, num:String, `type`:String,
                       picto:String, attribut_voie:String, voie:String,
                       heure: Long, etat:String, retard:String)

