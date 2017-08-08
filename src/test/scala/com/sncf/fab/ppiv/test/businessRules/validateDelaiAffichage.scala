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
class validateDelaiAffichage extends Specification{


  def is = s2"""
This is a specification for the "getAffichage" output
The 'getAffichageDuree1'  output   should
  be a positive number beginning from 0                                        $e1
  With trajet_sans_retard.csv the result should be 774                         $e2
  With trajet_avec_retard.csv data with delay the result should be 774         $e3
The 'getAffichageDuree2'  output   should
  be a positive number beginning from 0                                        $e4
  With trajet_sans_retard.csv the result should be 774                         $e5
  With trajet_avec_retard.csv with delay the result should be 1074             $e6
  """


  def readFile( file : String) = {
    for {
      line <- Source.fromFile(file).getLines()
      values = line.split(",",-1)
     } yield TgaTgdInput(values(0), values(1).toLong, values(2), values(3), values(4),values(5),values(6),values(7),values(8),values(9).toLong,values(10),values(11))
  }


  val sourcePipeline = new TraitementTga

  val header = Seq("gare","maj","train","ordes","num","type","picto","attribut_voie","voie","heure","etat","retard")
 // val pathSansRetard = new File("src/test/resources/data/trajet_sans_retard.csv").getAbsolutePath
   val pathSansRetard = new File("PPIV/src/test/resources/data/trajet_sans_retard.csv").getAbsolutePath
  //val pathAvecRetard = new File("src/test/resources/data/trajet_avec_retard.csv").getAbsolutePath()
  val pathAvecRetard = new File("PPIV/src/test/resources/data/trajet_avec_retard.csv").getAbsolutePath()



  val dsSansRetard = readFile(pathSansRetard).toSeq
  val dsAvecRetard = readFile(pathAvecRetard).toSeq


 val test = BusinessRules.getPremierAffichage(dsSansRetard)

  def e1 = BusinessRules.getAffichageDuree1(dsSansRetard).toInt must be_>= (0)
  def e2 = BusinessRules.getAffichageDuree1(dsSansRetard) mustEqual 774
  def e3 = BusinessRules.getAffichageDuree1(dsAvecRetard) mustEqual 774

  def e4 = BusinessRules.getAffichageDuree2(dsSansRetard).toInt must be_>= (0)
  def e5 = BusinessRules.getAffichageDuree2(dsSansRetard) mustEqual 774
  def e6 = BusinessRules.getAffichageDuree2(dsAvecRetard) mustEqual 1074

}