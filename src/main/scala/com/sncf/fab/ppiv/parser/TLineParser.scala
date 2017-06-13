package com.sncf.fab.ppiv.parser

/**
  * Created by simoh-labdoui on 11/05/2017.
  */

trait TLineParser[T] {
  def parseLine(logLine: String): Option[T]
}