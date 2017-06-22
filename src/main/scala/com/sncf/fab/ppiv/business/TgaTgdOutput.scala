package com.sncf.fab.ppiv.business

import java.sql.{Date, Timestamp}

import org.joda.time.DateTime


/**
  * Created by simoh-labdoui on 11/05/2017.
  * la table finale qui résume la qualité d'affichage des trains dans les gares
  */

@serializable case class TgaTgdOutput(nom_de_la_gare: String, agence: String, segmentation:  String,
                        uic: String, x: String, y: String, id_train: String,
                        num_train: String, `type`: String, origine_destination: String, type_panneau: String,
                        dateheure2: String)
