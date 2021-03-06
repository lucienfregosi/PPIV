package com.sncf.fab.ppiv.test.businessRules

import java.io.File
import java.util.concurrent.TimeUnit

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.pipelineData.TraitementTga
import com.sncf.fab.ppiv.pipelineData.libPipeline.BusinessRules
import org.specs2.Specification

import scala.io.Source

/**
  * Created by ELFI03951 on 04/07/2017.
  */
class DernierPremierAffichageSpec extends Specification{


  def is = s2"""
This is a specification for the "getPremierAffichage"  and "getDernierAffichage"output
  Dernier affichage  should be a equal to   1498948063                              $e1
  Premier affichage  should  be a equal to  1498947708                             $e2
  Premier affichage should be equal to 1504428183                                  $e3
  """


  def readFile( file : String) = {
    for {
      line <- Source.fromFile(file).getLines()
      values = line.split(",",-1)
     } yield TgaTgdInput(values(0), values(1).toLong, values(2), values(3), values(4),values(5),values(6),values(7),values(8),values(9).toLong,values(10),values(11))
  }

  //Path to file
  val pathDernierAffichage = new File("src/test/resources/data/businessRules/ExampleOfEventsForPremier_DernierAffichageTest.csv").getAbsolutePath
  // val pathDernierAffichage = new File("PPIV/src/test/resources/data/ExampleOfEventsForPremier_DernierAffichageTest.csv").getAbsolutePath

  val path = new File("src/test/resources/data/businessRules/test.csv").getAbsolutePath
  val pathRecette = new File("src/test/resources/data/validateData/recette.csv").getAbsolutePath

  val file  = readFile(path).toSeq
  val fileRecette = readFile(pathRecette).toSeq

  val retard = BusinessRules.getCycleRetard (file)

  val fileFiltered = file.filter(x=>( x.maj < x.heure + retard))

  //fileFiltered.foreach(println)

  // Bon affichage : 1503081412


  //println(BusinessRules.getPremierAffichage(fileFiltered))

  //Load File
  val dsDernierAffichage = readFile(pathDernierAffichage).toSeq
  val dsRecette = readFile(pathRecette).toSeq


  def e1 = BusinessRules.getDernierAffichage(dsDernierAffichage).toString must beEqualTo("1498948063")
  def e2 = BusinessRules.getPremierAffichage(dsDernierAffichage).toString must beEqualTo("1498947708")
  def e3 = BusinessRules.getPremierAffichage(dsRecette).toString must beEqualTo("1504428182")


}