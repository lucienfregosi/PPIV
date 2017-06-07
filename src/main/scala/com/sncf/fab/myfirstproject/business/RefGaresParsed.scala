package com.sncf.fab.myfirstproject.business

import java.sql.Date

/**
  * Created by smida-bassem on 11/05/2017.
  * Table issue des fichiers ref gares filtrer et nettoyée (Avec formattage des champs)
  */

case class RefGaresParsed(tvs:String,uic: String, gare: String, agence_gc: String,
                          segment_drg: String, longitude_ws_84: String, latitude_ws_84: String)
