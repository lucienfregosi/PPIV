CREATE EXTERNAL TABLE IF NOT EXISTS ppiv_ref.iv_tgatgd_rejet_cycle(
          cycleid string,
		  gare string,
		  origine_destination string,
		  num_train string,
		  type_train string,
		  dateheure2 bigint,
		  etat_train string,
		  premieraffichage bigint,
		  affichageduree1 bigint,
		  dernier_retard_annonce bigint,
		  affichageduree2 bigint,
		  affichage_retard bigint,
		  affichage_duree_retard bigint,
		  date_affichage_etat_train bigint,
		  delai_affichage_etat_train_avant_depart_arrive bigint,
		  dernier_quai_affiche string,
		  type_devoiement string,
		  type_devoiement2 string,
		  type_devoiement3 string,
		  type_devoiement4 string,
		  dernier_affichage bigint,
		  date_process bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/data1/GARES/refinery/PPIV_PHASE2/hive/iv_tgatgd_rejet_cycle2'