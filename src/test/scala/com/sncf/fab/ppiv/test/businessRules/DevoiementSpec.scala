package com.sncf.fab.ppiv.test.businessRules

import java.io.File

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.pipelineData.libPipeline.BusinessRules
import org.specs2.Specification

import scala.io.Source

/**
  * Created by ELFI03951 on 04/07/2017.
  */
class DevoiementSpec extends Specification{


  def is = s2"""
This is a specification for the "Devoiement Spec" output
The 'Devoiement Spec'  output   should
  premier devoiement must be a equal to  Affiche                           $e1
  second devoiement be a equal to  Affiche                                 $e2

  """


  def readFile( file : String) = {
    for {
      line <- Source.fromFile(file).getLines()
      values = line.split(",",-1)
    } yield TgaTgdInput(values(0), values(1).toLong, values(2), values(3), values(4),values(5),values(6),values(7),values(8),values(9).toLong,values(10),values(11))
  }


  val header = Seq("gare","maj","train","ordes","num","type","picto","attribut_voie","voie","heure","etat","retard")

 // val pathDevoiFile = new File("PPIV/src/test/resources/data/trajet_Devoiement.csv").getAbsolutePath
  val pathDevoiFile = new File("src/test/resources/data/trajet_Devoiement.csv").getAbsolutePath
  val dsDevoiSpec = readFile(pathDevoiFile).toSeq



  //BusinessRules.getTypeDevoiement(dsDevoiSpec).toString
  val test = BusinessRules.getTypeDevoiement(dsDevoiSpec).split("-")

  def e1 = BusinessRules.getTypeDevoiement(dsDevoiSpec).split("-") (2).toString.replaceAll("\\s", "") must beEqualTo("Affiche")

  def e2 = BusinessRules.getTypeDevoiement2(dsDevoiSpec).split("-") (2).toString.replaceAll("\\s", "") must beEqualTo("Affiche")



  //def e1 = "true" must beEqualTo("true")

}