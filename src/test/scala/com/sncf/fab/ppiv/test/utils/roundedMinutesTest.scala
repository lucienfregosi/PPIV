package com.sncf.fab.ppiv.test.utils

import com.sncf.fab.ppiv.utils.Conversion
import org.specs2.Specification

/**
  * Created by ELFI03951 on 28/09/2017.
  */
class roundedMinutesTest extends Specification{

  def is =
    s2"""

This is a specification fot the "roundMinutes"
The 'roundMinutes'  output   should
  be equal to 1 for 7 minutes ans 5 seconds                  $e1

  """

  // Affichage durant 7 minutes et 5 secondes (en milisecondes)
  val timestamp = (7 * 60 + 5) * 1000

  // Affichage durant 12 secondes (en milisecondes)
  val timestamp1 = 12 * 1000



  def e1 = Conversion.getMinutesRounded(timestamp) must beEqualTo(7)
  //def e2 = Conversion.getMinutesRounded(timestamp1) must beEqualTo(0)

}
