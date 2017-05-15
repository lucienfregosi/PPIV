package com.sncf.fab.myfirstproject.business

import java.sql.{Date, Timestamp}

import org.joda.time.DateTime


/**
  * Created by simoh-labdoui on 11/05/2017.
  * la table finale qui résume la qualité d'affichage des trains dans les gares
  */

case class QualiteAffichage(nomGare: String, agence: String, date_affichage:  String, uic: Int, typeTrain: String, typePanneau: String, origineDestination: String, depart: Boolean, arrive: Boolean, retard: Boolean, devoiement: Boolean)
