package com.sncf.fab.ppiv.utils

import java.text.{DecimalFormat, ParseException, SimpleDateFormat}
import java.util.concurrent.TimeUnit

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}
import java.util.{Calendar, Date}

import org.apache.hive.common.util.DateUtils

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
object Conversion {

  //DateTimeZone.setDefault(DateTimeZone.UTC)
  DateTimeZone.setDefault(DateTimeZone.forID("Europe/Paris"))

  val ParisTimeZone: DateTimeZone = DateTimeZone.forID("Europe/Paris")
  val timestampFormatWithTZ: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZone(ParisTimeZone)
  val timestampFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withZoneUTC()
  val yearMonthFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMM").withZoneUTC()
  val yearMonthDayFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC()
  val hoursFormat: DateTimeFormatter = DateTimeFormat.forPattern("HH").withZoneUTC()
  val HourMinuteFormat: DateTimeFormatter = DateTimeFormat.forPattern("HHmm").withZoneUTC()


  private def cleanTimeZone(timestamp: String): String = {
    timestamp.split('.')(0)
  }

  protected def now(): DateTime = {
    new DateTime(new Date(), ParisTimeZone)
  }

  def nowToDateTime(): DateTime = {

    now()
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
    new DateTime(time, ParisTimeZone)
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


  def getHourMax(date: DateTime): String = {
    // Retrancher une heure à la date actuelle pour traiter fichier à H-1
    //val HourToProcess = date.plusHours(-3)
    val HourToProcess = date.plusHours(-1)
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

 // def unixTimestampToDateTime(time: Long): DateTime = DateTime.parse(timestampFormatWithTZ.print(time * 1000), timestampFormatWithTZ).withZone(ParisTimeZone)
 def unixTimestampToDateTime(time: Long): DateTime = new DateTime(time * 1000L, DateTimeZone.forID("Europe/Paris"))

  def unixTimestampToDateTimeGMT(time: Long): DateTime = DateTime.parse(timestampFormatWithTZ.print(time * 1000), timestampFormatWithTZ)
  def escapeSimpleQuote(line: String): String = {
    line.replace("'", "\\'")
  }


  def getHHmmss(timestamp: Long): String = {
    val dateTime = Conversion.unixTimestampToDateTime(timestamp)
    val fmt = DateTimeFormat.forPattern("HH:mm:ss")
    val HHmmss = fmt.print(dateTime)
    HHmmss
  }


  def getHHmmssFromMillis(timestamp: Long): String = {
    val H = TimeUnit.MILLISECONDS.toHours(timestamp * 1000)
    val m = TimeUnit.MILLISECONDS.toMinutes(timestamp * 1000  - H * 60 * 60 * 1000)
    val s = TimeUnit.MILLISECONDS.toSeconds(timestamp * 1000- H * 60 * 60 * 1000 - m * 60 * 1000)
    val HH =  new DecimalFormat("00").format(H)
    val mm =  new DecimalFormat("00").format(m)
    val ss =  new DecimalFormat("00").format(s)
    HH+":"+mm+":"+ss
  }


  def getYYYYmmdd(timestamp: Long): String = {
    val dateFormat =  new SimpleDateFormat("YYYY-MM-DD")
    dateFormat.format(timestamp)
  }

  def HourFormat  ( hour : Int ): String= {
    new DecimalFormat("00").format(hour)
  }

  def validateDateInputFormat(date: String): Boolean = try {
    // Création du dateFormat adapté au format que l'on veut voir en entrée
    val df = new SimpleDateFormat("yyyyMMdd_HH")

    // Pour que la vérification soit plus stricte
    df.setLenient(false)
    df.parse(date)

    true
  } catch {
    case e: ParseException => false
  }


  def getDateTimeFromArgument(date: String):DateTime = {
    val df = DateTimeFormat.forPattern("yyyyMMdd_HH")
    // On renvoie le dateTime
    df.parseDateTime(date)
  }

}
