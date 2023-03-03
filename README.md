1) Download Data:

wget https://www.kaggle.com/datasets/abcsds/pokemon

2) create image:

 docker build -t omg .

3) run container:

docker run -it --name spark-cluster -p 4040:4040 -p 8080:8080 -v /Users/kbandaru/Documents/airflow-materials/pokeman/mnt/:/mnt/ omg /bin/bash

4) submit job:

cd /mnt/scripts
spark-submit pokeman.py  >> pokeman.log 2>&1

5) To remove all images:

docker rmi -f $(docker images -a -q)

6) See output in log

    1) what are the top 5 strongest non-legendary monsters?


    +---+-----------------------+------+-------+-----+---+------+-------+-------+-------+-----+----------+---------+
    |#  |Name                   |Type 1|Type 2 |Total|HP |Attack|Defense|Sp. Atk|Sp. Def|Speed|Generation|Legendary|
    +---+-----------------------+------+-------+-----+---+------+-------+-------+-------+-----+----------+---------+
    |445|GarchompMega Garchomp  |Dragon|Ground |700  |108|170   |115    |120    |95     |92   |4         |false    |
    |376|MetagrossMega Metagross|Steel |Psychic|700  |80 |145   |150    |105    |110    |110  |3         |false    |
    |373|SalamenceMega Salamence|Dragon|Flying |700  |95 |145   |130    |120    |90     |120  |3         |false    |
    |248|TyranitarMega Tyranitar|Rock  |Dark   |700  |100|164   |150    |95     |120    |71   |2         |false    |
    |289|Slaking                |Normal|null   |670  |150|160   |100    |95     |65     |100  |3         |false    |
    +---+-----------------------+------+-------+-----+---+------+-------+-------+-------+-----+----------+---------+

    2) Which Pokemon type has the highest average HP?

    The type with the highest average HP is: Dragon with an average HP of 83.3125

    3) Which is the most common special Attack?

    The most common special attack is:	  Water
