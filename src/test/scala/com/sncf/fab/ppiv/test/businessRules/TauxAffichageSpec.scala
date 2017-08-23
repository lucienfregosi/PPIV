package com.sncf.fab.ppiv.test.businessRules
import java.io.File

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.pipelineData.libPipeline.{BusinessConversion, BusinessRules}
import org.specs2.Specification
import scala.io.Source


class TauxAffichageSpec extends Specification  {


    def is =  s2"""
This is a specification for the "getTauxAffichage"

 Taux Affichage must  be equal to 0                            $e1
 Taux AFfichage (retard inclus ) must be equal to to 1         $e2
   """



    def readFile( file : String) = {
      for {
        line <- Source.fromFile(file).getLines()
        values = line.split(",",-1)
      } yield TgaTgdInput(values(0), values(1).toLong, values(2), values(3), values(4),values(5),values(6),values(7),values(8),values(9).toLong,values(10),values(11))
    }

    //Path to file

    val pathDureeAffichage2 = new File("src/test/resources/data/businessRules/ExampleOfEventForTauxAffichage.csv").getAbsolutePath


    //Load File
    val dsDureeAffichage2= readFile(pathDureeAffichage2).toSeq

    val dureeAffichage1 = BusinessRules.getAffichageDuree1(dsDureeAffichage2)
    val dureeAffichage2  = BusinessRules.getAffichageDuree2(dsDureeAffichage2)



    def e1 =  BusinessConversion.getTauxAffichage(dureeAffichage1)  must beEqualTo(0)
    def e2 =  BusinessConversion.getTauxAffichage(dureeAffichage2)  must beEqualTo(1)



}
