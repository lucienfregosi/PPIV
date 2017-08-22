package com.sncf.fab.ppiv.test.businessRules

import java.io.File

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.pipelineData.libPipeline.BusinessRules
import org.specs2.Specification

import scala.io.Source

/**
  * Created by ELFI03951 on 22/08/2017.
  */
class affichageDuree1 extends Specification {


  def is = s2"""
This is a specification for the affichageDuree1 output
  duree shoud be < 25                            $e1
  """


  def readFile( file : String) = {
    for {
      line <- Source.fromFile(file).getLines()
      values = line.split(",",-1)
    } yield TgaTgdInput(values(0), values(1).toLong, values(2), values(3), values(4),values(5),values(6),values(7),values(8),values(9).toLong,values(10),values(11))
  }

  //Path to file
  val pathDureeAffichage = new File("src/test/resources/data/testAffichageDuree1").getAbsolutePath



  //Load File
  val dsDureeAffichage= readFile(pathDureeAffichage).toSeq


  // 1. On remplace les valeurs vides par ~
  def e1 = (BusinessRules.getAffichageDuree1(dsDureeAffichage).toInt / 60) must beEqualTo(19)


}
