
Projet PPIV phase 2

Par défaut le projet se lance en configuration du serveur OVH "en prod" (mais en utilisant refinery à la place de Gold)

Pour lancer le projet en local : sbt -Dconfig.resource=local.conf "run fs"
Pour lancer le projet normalement sbt "run hdfs"

!! Ne pas oublier en local de renommer les fichiers avec l'heure et le jour actuel pour que le pipeline puisse se lancer.


Liste des paramètres pris en charge lors du lancement
Arg[0] : Moyen de persistance des données (hive, hdfs, ElasticSearch ou en local)
Arg[1] et Arg[2] spécifie une plage horaire pour lequel lancer le batch si l'on veut jouer plusieurs heures de la journée
ils suivent le format suivant YYYYMMDD_HH

Par exemple spark-submit ... hive 20170807_15 20170808_03 va processer toutes les données du 07 aout 2017 à 15h jusqu'au
08 aout 2017 a 3h.

Si l'on ne renseigne pas ces paramètres le programme sera lancé par défaut pour processer l'heure n-1 (le fonctionnement nominal)

