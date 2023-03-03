1) Download Data
<!-- wget https://www.kaggle.com/datasets/abcsds/pokemon -->

2) create image
<!--  docker build -t omg . -->

3) run container
<!-- docker run -it --name spark-cluster -p 4040:4040 -p 8080:8080 -v /Users/kbandaru/Documents/airflow-materials/pokeman/mnt/:/mnt/ omg /bin/bash -->

4) submit job 
<!-- cd /mnt/scripts
spark-submit pokeman.py  >> pokeman.log 2>&1 -->

5) To remove all images
<!-- docker rmi -f $(docker images -a -q) -->