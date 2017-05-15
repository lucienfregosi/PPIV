package com.sncf.fab.myfirstproject.parser

import com.sncf.fab.myfirstproject.Exception.PpivRejectionHandler
import com.sncf.fab.myfirstproject.business.TgaTgdParsed
import java.sql.Date
import java.util.Calendar


/**
  * Created by simoh-labdoui on 11/05/2017.
  * Parser chaque ligne des fichiers TGA/TGD
  */
class TgParseLine extends TLineParser[TgaTgdParsed] {

  override def parseLine(logLine: String): Option[TgaTgdParsed] = {
    val sysOrigine = "logline"
    val date=Date.valueOf((Calendar.getInstance().getTime().getTime()).toString)
    try {
      Some(

          TgaTgdParsed("",0,"","","","","","0","",0,"","0"  )

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