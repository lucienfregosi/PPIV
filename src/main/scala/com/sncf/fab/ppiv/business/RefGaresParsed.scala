package com.sncf.fab.ppiv.business

import java.sql.Date

/**
  * Created by smida-bassem on 11/05/2017.
  * Table issue des fichiers ref gares filtrer et nettoyée (Avec formattage des champs)
  */

case class RefGaresParsed(CodeGare: String,
                          IntituleGare: String,
                          NombrePlateformes: String,
                          SegmentDRG: String,
                          UIC: String,
                          UniteGare: String,
                          TVS: String,
                          CodePostal: String,
                          Commune: String,
                          DepartementCommune: String,
                          Departement: String,
                          Region: String,
                          AgenceGC: String,
                          RegionSNCF: String,
                          NiveauDeService: String,
                          LongitudeWGS84: String,
                          LatitudeWGS84: String,
                          DateFinValiditeGare: String
                         )
