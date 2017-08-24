CREATE EXTERNAL TABLE IF NOT EXISTS ppiv_ref.iv_tgatgd3days2(
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
                            mois String,
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
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/data1/GARES/refinery/PPIV_PHASE2/hive/3days2'

ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:nom_de_la_gare'='CHAR(90)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:agence'='CHAR(30)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:segmentation'='CHAR(3)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:uic'='CHAR(10)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:x'='FLOAT(8)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:y'='FLOAT(8)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:id_train'='CHAR(25)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:num_train'='CHAR(10)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:type'='CHAR(12)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:origine_destination'='CHAR(40)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:type_panneau'='CHAR(3)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:premier_affichage'='DATETIME(25.6)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:date_extract'='CHAR(10)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:mois'='CHAR(10)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:annee'='CHAR(4)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:dateheure2'='DATETIME(25.6)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:creneau_horaire'='CHAR(25)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:jour_depart_arrivee'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:jour_depart_arrivee1'='CHAR(3)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:affichage_duree1'='CHAR(12)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:affichage_duree1_minutes'='CHAR(5)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:delai_affichage_voie_sans_retard'='CHAR(10)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:duree_temps_affichage'='CHAR(15)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:nb_retard1'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:dernier_retard_annonce_min'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:nb_retard2'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:dernier_retard_annonce'='CHAR(9)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:affichage_duree_2minutes'='CHAR(5)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:affichage_duree_2'='CHAR(12)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:delai_affichage_voie_avec_retard'='CHAR(10)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:duree_temps_affichage2'='CHAR(15)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:taux_affichage'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:taux_affichage2'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:affichage_retard'='DATETIME(25.6)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:affichage_duree_retard'='TIME(15.6)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:etat_train'='CHAR(5)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:date_affichage_etat_train'='DATETIME(25.6)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:delai_affichage_etat_train_avant_depart_arrive'='CHAR(12)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:delai_affichage_etat_train_avant_depart_arrive_min'='CHAR(10)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:quai_devoiement'='CHAR(10)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:quai_devoiement2'='CHAR(10)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:quai_devoiement3'='CHAR(10)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:quai_devoiement4'='CHAR(10)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:dernier_quai_affiche'='CHAR(4)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:devoiement'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:devoiement_affiche'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:devoiement_non_affiche'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:type_devoiement'='CHAR(20)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:type_devoiement2'='CHAR(20)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:type_devoiement3'='CHAR(20)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:type_devoiement4'='CHAR(20)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:carac_devoiement'='CHAR(20)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:dernier_affichage'='DATETIME(25.6)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:date_process'='DATETIME(25.6)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:affichage_duree_retard_minutes'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:delai_affichage_duree_retard'='CHAR(10)');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:taux_affichage_30'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:taux_affichage2_30'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:taux_affichage_45'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:taux_affichage2_45'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:taux_affichage_15'='SMALLINT');
ALTER TABLE ppiv_ref.iv_tgatgd SET TBLPROPERTIES ('SASFMT:taux_affichage2_15'='SMALLINT');
