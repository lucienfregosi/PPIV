package com.sncf.fab.ppiv.Exception

/**
  * Created by simoh-labdoui on 11/05/2017.
  */

import org.apache.log4j.Logger

object PpivRejectionHandler extends Serializable {
  var LOGGER = Logger.getLogger(PpivRejectionHandler.getClass)


  val UNKNOWN_LOGIN_IN_DWH_ERROR = 0
  val PARSING_ERROR = 1
  val TYPE_PARSING_ERROR = 2
  val PROCESSING_ERROR = 3

  val causes = Map[Int, String](
    UNKNOWN_LOGIN_IN_DWH_ERROR -> "uic du train non trouvé",
    PARSING_ERROR -> "Erreur de parsing",
    TYPE_PARSING_ERROR -> "format de fichier a changé"
  )

  /**
    *
    * @param sysOrigine tga ou tgd
    * @param erroCode le type d'erreur
    */
  def handleRejection(sysOrigine: String, erroCode: Int): Unit = {
    val errorMess: String = causes.getOrElse(erroCode, "UNKNOWN")
    LOGGER.info(sysOrigine + " " + erroCode)

  }
}
