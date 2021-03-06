CREATE EXTERNAL TABLE IF NOT EXISTS ppiv_ref.iv_tgatgdTestPartition(
                            nom_de_la_gare String,
                            agence String,
                            segmentation String,
                            uic String,
                            x Float,
                            y Float,
                            id_train String,
                            num_train String,
                            type String,
                            origine_destination String,
                            type_panneau String,
                            premier_affichage String,
                            date_extract String,
                            annee String,
                            dateheure2 String,
                            creneau_horaire String,
                            jour_depart_arrivee Int,
                            jour_depart_arrivee1 String,
                            affichage_duree1 String,
                            affichage_duree1_minutes String,
                            delai_affichage_voie_sans_retard String,
                            duree_temps_affichage String,
                            nb_retard1 Int,
                            dernier_retard_annonce_min Int,
                            nb_retard2 Int,
                            dernier_retard_annonce String,
                            affichage_duree_2minutes String,
                            affichage_duree_2 String,
                            delai_affichage_voie_avec_retard String,
                            duree_temps_affichage2 String,
                            taux_affichage Int,
                            taux_affichage2 Int,
                            affichage_retard String,
                            affichage_duree_retard String,
                            etat_train String,
                            date_affichage_etat_train String,
                            delai_affichage_etat_train_avant_depart_arrive String,
                            delai_affichage_etat_train_avant_depart_arrive_min String,
                            quai_devoiement String,
                            quai_devoiement2 String,
                            quai_devoiement3 String,
                            quai_devoiement4 String,
                            dernier_quai_affiche String,
                            devoiement Int,
                            devoiement_affiche Int,
                            devoiement_non_affiche Int,
                            type_devoiement String,
                            type_devoiement2 String,
                            type_devoiement3 String,
                            type_devoiement4 String,
                            carac_devoiement String,
                            dernier_affichage String,
                            date_process String,
                            affichage_duree_retard_minutes Int,
                            delai_affichage_duree_retard String,
                            taux_affichage_30 Int,
                            taux_affichage2_30 Int,
                            taux_affichage_45 Int,
                            taux_affichage2_45 Int,
                            taux_affichage_15 Int,
                            taux_affichage2_15 Int
)
PARTITIONED BY(mois STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/data1/GARES/refinery/PPIV_PHASE2/hive/iv_tgatgdTestPartition'



