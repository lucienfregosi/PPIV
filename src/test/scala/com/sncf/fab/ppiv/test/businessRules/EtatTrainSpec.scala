package com.sncf.fab.ppiv.test.businessRules

import java.io.File

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.pipelineData.libPipeline.BusinessRules
import com.sncf.fab.ppiv.utils.Conversion
import org.specs2.Specification

import scala.io.Source

/**
  * Created by ELFI03951 on 04/07/2017.
  */
class EtatTrainSpec extends Specification{


  def is = s2"""
This is a specification for the "Etat Train" output
The 'Etat Train'  output   should
  Etat Train must be SUP                                                 $e1
  Maj of Etat Train must be equal to  1499076966                         $e2
  Date Affichage etat train   must be equal to affichage_etat_train      $e3
  """


  def readFile( file : String) = {
    for {
      line <- Source.fromFile(file).getLines()
      values = line.split(",",-1)
     } yield TgaTgdInput(values(0), values(1).toLong, values(2), values(3), values(4),values(5),values(6),values(7),values(8),values(9).toLong,values(10),values(11))
  }


  val header = Seq("gare","maj","train","ordes","num","type","picto","attribut_voie","voie","heure","etat","retard")

  val pathEtatFile = new File("src/test/resources/data/trajet_avec_etatTrain.csv").getAbsolutePath
  val dsEtatSpec = readFile(pathEtatFile).toSeq

val affichage_etat_train = (1499077020 - 1499076966).toString

  def e1 = BusinessRules.getEtatTrain(dsEtatSpec).toString must beEqualTo("SUP")
  def e2 = BusinessRules.getDateAffichageEtatTrain(dsEtatSpec).toString must beEqualTo("1499076966")
  def e3 = BusinessRules.getDelaiAffichageEtatTrainAvantDepartArrive(dsEtatSpec).toString must beEqualTo(affichage_etat_train)


}