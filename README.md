
Projet PPIV phase 2

Par défaut le projet se lance en configuration du serveur OVH "en prod" (mais en utilisant refinery à la place de Gold)

Pour lancer le projet en local : sbt -Dconfig.resource=local.conf "run hdfs"

!! Ne pas oublier en local de renommer les fichiers avec l'heure et le jour actuel pour que le pipeline puisse se lancer.