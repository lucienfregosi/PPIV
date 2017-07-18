package com.sncf.fab.ppiv.test.businessRules

import java.io.File

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
This is a specification for the "getDernierAffichage" output
The 'getDernierAffichage'  output   should
  Dernier affichage be a equal to  1498948063                             $e1
  Premier affichage be a equal to  1498946344                             $e2
  """


  def readFile( file : String) = {
    for {
      line <- Source.fromFile(file).getLines()
      values = line.split(",",-1)
     } yield TgaTgdInput(values(0), values(1).toLong, values(2), values(3), values(4),values(5),values(6),values(7),values(8),values(9).toLong,values(10),values(11))
  }


  val header = Seq("gare","maj","train","ordes","num","type","picto","attribut_voie","voie","heure","etat","retard")

  val pathDernierAffichage = new File("src/test/resources/data/trajet_dernier_Affichage.csv").getAbsolutePath
  val dsDernierAffichage = readFile(pathDernierAffichage).toSeq


 // def e1 = BusinessRules.getDernierAffichage(dsDernierAffichage).toInt must be_>= (0)
  def e1 = BusinessRules.getDernierAffichage(dsDernierAffichage).toString must beEqualTo("1498948063")
  def e2 = BusinessRules.getPremierAffichage(dsDernierAffichage).toString must beEqualTo("1498946344")


}