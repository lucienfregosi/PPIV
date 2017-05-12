package com.sncf.fab.myfirstproject.parser

import com.sncf.fab.myfirstproject.Exception.PpivRejectionHandler
import com.sncf.fab.myfirstproject.business.TgaTgdParsed
import org.joda.time.DateTime


/**
  * Created by simoh-labdoui on 11/05/2017.
  * Parser chaque ligne des fichiers TGA/TGD
  */
class TgParseLine extends TLineParser[TgaTgdParsed] {

  override def parseLine(logLine: String): Option[TgaTgdParsed] = {
    val sysOrigine = "logline"
    try {
      Some(
        TgaTgdParsed (
          "", "", DateTime.now(), 0, "", "", "", true, true, true, true
        )
      )

    } catch {
      case e: Throwable => {
        PpivRejectionHandler.handleRejection(sysOrigine, PpivRejectionHandler.PARSING_ERROR)
        None
      }
    }
  }
}
object TgParseLine extends TgParseLine