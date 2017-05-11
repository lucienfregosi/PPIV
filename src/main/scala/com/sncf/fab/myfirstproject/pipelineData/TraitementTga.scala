package com.sncf.fab.myfirstproject.pipelineData

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
class TraitementTga extends SourcePipeline{

  override def getSource()="TGA"

}
object TraitementTga extends TraitementTga