package com.sncf.fab.ppiv.utils

import java.text.{DecimalFormat, SimpleDateFormat}

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}
import java.util.{Calendar, Date}

import org.apache.hive.common.util.DateUtils

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
object Conversion {

  DateTimeZone.setDefault(DateTimeZone.UTC)
  val timestampFormatWithTZ: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZoneUTC()
  val timestampFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withZoneUTC()
  val yearMonthFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMM").withZoneUTC()
  val yearMonthDayFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC()
  val hoursFormat: DateTimeFormatter = DateTimeFormat.forPattern("HH").withZoneUTC()
  val HourMinuteFormat: DateTimeFormatter = DateTimeFormat.forPattern("HHmm").withZoneUTC()
  val ParisTimeZone: DateTimeZone = DateTimeZone.forID("Europe/Paris")

  private def cleanTimeZone(timestamp: String): String = {
    timestamp.split('.')(0)
  }

  protected def now(): DateTime = {
    new DateTime(new Date(), ParisTimeZone)
  }

  def nowToDateTime(): DateTime = {
    now().plusDays(-2)
  }


  def nowToString(): String = {
    timestampFormat.print(now())
  }

  def nowToString(format: String): String = {
    val formatter = DateTimeFormat.forPattern(format).withZoneUTC()
    formatter.print(now())
  }

  def dateToDateTime(date: Date): DateTime = {
    new DateTime(date, DateTimeZone.UTC)
  }

  def dateToDateTime(date: Date, dateTimeZone: DateTimeZone): DateTime = {
    new DateTime(date, dateTimeZone).withZone(DateTimeZone.UTC)
  }

  def getDateTime(time: Long): DateTime = {
    new DateTime(time, DateTimeZone.UTC)
  }

  def getDateTime(time: Long, dateTimeZone: DateTimeZone): DateTime = {
    new DateTime(time, dateTimeZone).withZone(DateTimeZone.UTC)
  }

  def getDateTime(year: Int, month: Int, day: Int, hour: Int, min: Int, sec: Int): DateTime = {
    new DateTime(year, month, day, hour, min, sec, DateTimeZone.UTC)
  }

  def getDateTime(year: Int, month: Int, day: Int, hour: Int, min: Int, sec: Int, millis: Int): DateTime = {
    new DateTime(year, month, day, hour, min, sec, millis, DateTimeZone.UTC)
  }

  def getDateTime(year: Int, month: Int, day: Int, hour: Int, min: Int, sec: Int, dateTimeZone: DateTimeZone): DateTime = {
    new DateTime(year, month, day, hour, min, sec, dateTimeZone).withZone(DateTimeZone.UTC)
  }

  def getDateTime(year: Int, month: Int, day: Int, hour: Int, min: Int, sec: Int, millis: Int, dateTimeZone: DateTimeZone): DateTime = {
    new DateTime(year, month, day, hour, min, sec, millis, dateTimeZone).withZone(DateTimeZone.UTC)
  }

  def getDateTimeIgnoreMsAndTZ(timestamp: String): DateTime = {
    timestampFormat.parseDateTime(cleanTimeZone(timestamp))
  }

  def getDateTime(timestamp: String): DateTime = {
    timestampFormatWithTZ.parseDateTime(timestamp)
  }

  def getDateTimeIgnoreMsAndTZ(timestamp: String, format: String): DateTime = {
    val formatter = DateTimeFormat.forPattern(format).withZoneUTC()
    formatter.parseDateTime(cleanTimeZone(timestamp))
  }

  def getDateTime(timestamp: String, format: String): DateTime = {
    val formatter = DateTimeFormat.forPattern(format).withZoneUTC()
    formatter.parseDateTime(timestamp)
  }

  def getDateTimeFromISO(isoTimestamp: String): DateTime = {
    DateTime.parse(isoTimestamp, ISODateTimeFormat.dateTimeParser().withZoneUTC())
  }

  def dateTimeToString(timestamp: DateTime): String = {
    timestampFormat.print(timestamp)
  }

  def dateTimeToString(timestamp: DateTime, format: String): String = {
    val formatter = DateTimeFormat.forPattern(format).withZoneUTC()
    formatter.print(timestamp)
  }

  def getYearMonth(timestamp: String): Int = {
    yearMonthFormat.print(timestampFormat.parseDateTime(timestamp)).toInt
  }

  def getYearMonth(date: DateTime): Int = {
    yearMonthFormat.print(date).toInt
  }

  def getYearMonthDay(timestamp: String): Int = {
    yearMonthDayFormat.print(timestampFormat.parseDateTime(timestamp)).toInt
  }

  def getYearMonthDay(date: DateTime): Int = {
    yearMonthDayFormat.print(date).toInt
  }

  def getHour(date: DateTime): String = {
    // Retrancher une heure à la date actuelle pour traiter fichier à H-1
    val HourToProcess = date.plusHours(-2)
    // Convertir sous le format HH type 01 au lieu de 1
    println(new DecimalFormat("00").format(HourToProcess.getHourOfDay))
    new DecimalFormat("00").format(HourToProcess.getHourOfDay)
  }



  def getYesterdaysDate(): Int = {
    val ft = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val timestamp = cal.getTime
    ft.format(timestamp).toInt
  }

  def unixTimestampToDateTime(time: Long): DateTime = DateTime.parse(timestampFormatWithTZ.print(time * 1000), timestampFormatWithTZ)

  def escapeSimpleQuote(line: String): String = {
    line.replace("'", "\\'")
  }

  def getHHmmss(date: DateTime): String = {
    new SimpleDateFormat("HH:mm:ss").format(date)
  }
}
