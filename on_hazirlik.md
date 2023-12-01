```commandline
(base) [train@10 01_airflow_spark_sqoop]$ docker-compose up -d
(base) [train@10 01_airflow_spark_sqoop]$ docker exec -it spark_client bash
root@e9ab370b124e:/# ssh localhost
```
ssh: connect to host localhost port 22: Network is unreachable


```commandline
root@e9ab370b124e:/# apt update && apt install  openssh-server sudo -y
root@e9ab370b124e:/# useradd -rm -d /home/ssh_train -s /bin/bash -g root -G sudo -u 1000 ssh_train
```
useradd warning: ssh_train's uid 1000 is greater than SYS_UID_MAX 999

```commandline
root@e9ab370b124e:/# echo 'ssh_train:Ankara06' | chpasswd
root@e9ab370b124e:/# service ssh start
```
Starting OpenBSD Secure Shell server: sshd.

------------------------------------

**Bu kısım daha sonra airflow loglarında spark-submit çalıştırırken aldığını gördüğüm bir hata. ‘history’ dizininin ownership’ini ssh_train’e vermediği için spark loglarını history altına yazamıyordu. Manuel bir şekilde değiştirmem gerekti.**

```commandline
root@e9ab370b124e:/opt/spark# ls -l
```
total 112
-rw-r--r--. 1 ssh_train 1000 22982 Jun 19 23:23 LICENSE

-rw-r--r--. 1 ssh_train 1000 57842 Jun 19 23:23 NOTICE

drwxr-xr-x. 3 ssh_train 1000    17 Jun 19 23:23 R

-rw-r--r--. 1 ssh_train 1000  4605 Jun 19 23:23 README.md

-rw-r--r--. 1 ssh_train 1000   165 Jun 19 23:23 RELEASE

drwxr-xr-x. 2 ssh_train 1000  4096 Jun 19 23:23 bin

drwxr-xr-x. 1 ssh_train 1000    33 Oct  1 05:39 conf

drwxr-xr-x. 5 ssh_train 1000    50 Jun 19 23:23 data

drwxr-xr-x. 4 ssh_train 1000    29 Jun 19 23:23 examples

**drwxr-xr-x. 2 root      root     6 Oct  1 05:39 history**

drwxr-xr-x. 1 ssh_train 1000   103 Oct  1 05:39 jars

drwxr-xr-x. 4 ssh_train 1000    38 Jun 19 23:23 kubernetes

drwxr-xr-x. 2 ssh_train 1000  4096 Jun 19 23:23 licenses

drwxr-xr-x. 9 ssh_train 1000  4096 Jun 19 23:23 python

drwxr-xr-x. 2 ssh_train 1000  4096 Jun 19 23:23 sbin

drwxr-xr-x. 2 ssh_train 1000    42 Jun 19 23:23 yarn

```commandline
root@e9ab370b124e:/opt/spark# sudo chown -R ssh_train:1000 /opt/spark/history
```
---------------------------

```commandline
root@e9ab370b124e:/# python3 -m pip install virtualenv
root@e9ab370b124e:/home/ssh_train# python3 -m virtualenv datagen
root@e9ab370b124e:/home/ssh_train# git clone https://github.com/erkansirin78/data-generator.git
root@e9ab370b124e:/home/ssh_train# source datagen/bin/activate
(datagen) root@e9ab370b124e:/home/ssh_train# cd data-generator
(datagen) root@e9ab370b124e:/home/ssh_train/data-generator# pip install -r requirements.txt
(datagen) root@e9ab370b124e:/home/ssh_train/data-generator# cd ..
(datagen) root@e9ab370b124e:/home/ssh_train# mkdir datasets
(datagen) root@e9ab370b124e:/home/ssh_train# cd datasets
(datagen) root@e9ab370b124e:/home/ssh_train/datasets# wget -O tmdb_5000_movies_and_credits.zip https://github.com/erkansirin78/datasets/raw/master/tmdb_5000_movies_and_credits.zip
(datagen) root@e9ab370b124e:/home/ssh_train/datasets# unzip tmdb_5000_movies_and_credits.zip
(datagen) root@e9ab370b124e:/home/ssh_train/datasets# rm -r tmdb_5000_movies_and_credits.zip
```
---------------------------
**Spark - deltalake entegrasyonu için gerekli dependency’ler olan delta-core  ve delta-storage jarlarını manuel olarak indirmek yerine spark-submit esnasında da –packages option’ı ile de verebilirdik.**

```commandline
(datagen) root@e9ab370b124e:/# ls -l /opt/spark/jars
(datagen) root@e9ab370b124e:/# wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar &&\
mv delta-core_2.12-2.4.0.jar opt/spark/jars/ &&\
wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar &&\
mv delta-storage-2.4.0.jar opt/spark/jars/
```


