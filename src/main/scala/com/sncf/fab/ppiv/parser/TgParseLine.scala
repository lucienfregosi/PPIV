package com.sncf.fab.ppiv.parser

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.business.TgaTgdInput
import java.sql.Date
import java.util.Calendar


/**
  * Created by simoh-labdoui on 11/05/2017.
  * Parser chaque ligne des fichiers TGA/TGD
  */
class TgParseLine extends TLineParser[TgaTgdInput] {

  override def parseLine(logLine: String): Option[TgaTgdInput] = {
    val sysOrigine = "logline"
    val date=Date.valueOf((Calendar.getInstance().getTime().getTime()).toString)
    try {
      Some(
          TgaTgdInput("",0,"","","","","","","",0,"","")
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