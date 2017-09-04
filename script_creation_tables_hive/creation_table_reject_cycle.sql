CREATE EXTERNAL TABLE IF NOT EXISTS ppiv_ref.iv_tgatgd_rejet_cycle(
                       cycleId  String,
                              gare String,
                              origine_destination String,
                              num_train String,
                              type_train String,
                              dateheure2 BIGINT,
                              etat_train String,
                              premierAffichage BIGINT,
                              affichageDuree1 BIGINT,
                              dernier_retard_annonce BIGINT,
                              affichageDuree2 BIGINT,
                              affichage_retard BIGINT,
                              affichage_duree_retard BIGINT,
                              date_affichage_etat_train BIGINT,
                              delai_affichage_etat_train_avant_depart_arrive BIGINT,
                              dernier_quai_affiche String,
                              type_devoiement String,
                              type_devoiement2 String,
                              type_devoiement3String,
                              type_devoiement4 String,
                              dernier_affichage: BIGINT,
                              date_process BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/data1/GARES/refinery/PPIV_PHASE2/hive/iv_tgatgd_rejet_cycle'


