package com.sncf.fab.myfirstproject.parser

import com.sncf.fab.myfirstproject.Exception.RejectionHandler
import com.sncf.fab.myfirstproject.business.TgaTgd
import org.joda.time.DateTime


/**
  * Created by simoh-labdoui on 11/05/2017.
  * Parser chaque ligne des fichiers TGA/TGD
  */
class TgParseLine extends TLineParser[TgaTgd] {

  override def parseLine(logLine: String): Option[TgaTgd] = {
    val sysOrigine = "logline"
    try {
      Some(
        TgaTgd (
          "", "", DateTime.now(), 0, "", "", "", true, true, true, true
        )
      )

    } catch {
      case e: Throwable => {
        RejectionHandler.handleRejection(sysOrigine, RejectionHandler.PARSING_ERROR)
        None
      }
    }
  }
}
object TgParseLine extends TgParseLine