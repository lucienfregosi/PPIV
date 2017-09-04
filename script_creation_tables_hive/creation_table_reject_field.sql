CREATE EXTERNAL TABLE IF NOT EXISTS ppiv_ref.iv_tgatgd_rejet_field(
	  gare   String,
	  maj    BIGINT,
	  train  String,
      ordes  String,
	  num    String,
	  type   String,
      picto  String,
	  attribut_voie String,
	  voie   String,
      heure  BIGINT,
	  etat   String,
	  retard String
	  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/data1/GARES/refinery/PPIV_PHASE2/hive/iv_tgatgd_reject_field';
