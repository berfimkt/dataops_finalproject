## Dataops Final Projesi Adımlar


### 1-	Ön hazırlık: Komutları ‘ön hazırlık’ isimli dosyada yer alıyor.

    •	Daha sonrasında airflow arayüzünde spark_client container’ına bağlantı kurabilmek için bir ssh bağlantısı tanımlayacağız. Airflow’dan spark_client container’ına ssh bağlantısını kurarken ssh_train isimli bir kullanıcı ile yapacağız bu işi.
    •	Dolayısıyla iki şeye ihtiyacımız var; birincisi spark_client container’ında 22 portunu dinleyen ve bizim isteklerimizi kabul edecek bir sshd servisi, ikincisi ise ssh_train isimli bir kullanıcı.
    •	Bir python virtual environment’ı yaratalım, önce yüklememiz gerekiyor.Virtual environment’ı aktive edelim. Ardından requirements’ı kuralım.
    •	MinIO’ya data-generate etme işini aşağıdaki repoda bulunan script’i çalıştırarak yapacağız. Bu repoyu indirelim.

        https://github.com/erkansirin78/data-generator/blob/master/dataframe_to_s3.py

    •	Generate edeceğimiz verisetini indirelim.

        https://github.com/erkansirin78/datasets/raw/master/tmdb_5000_movies_and_credits.zip


### 2-	data-generator ile MinIO’ya veri göndermek için kullanabileceğimiz optionlara bakalım. Bu aşamaya ait, Airflow DAG’ımızda credits ve movies datasetleri için iki ayrı task olacak ve paralel çalışacaklar[t1,t2].

    python dataframe_to_s3.py –help


### 3-	MinIO’dan Apache Spark’a veriyi okumak, üzerinden dönüşümler yapmak ve delta formatında MinIO’ya geri yazmak için kodlar final_project.ipynb dosyasında bulunuyor.

    •	Aynı compose dosyasında bulunan bir diğer service ‘minio’. Spark kodlarımız ise spark_client container’ında çalışacak. Aynı compose dosyası içerisinde bulunan servisler birbirine servis isimleriyle erişebiliyor dolayısıyla spark_client – minio bağlantısı kurarken endpoint’imizi buna göre düzenliyoruz.
    •	db_conn isimli dosyada ise MinIO arayüzüne bağlanırken kullanacağımız credential’lar mevcut, bunları kodumuza açık bir şekilde eklememek için configparser ile değişkenlere atıyoruz.

### 4-	Tüm bunları orchestrate edeceğimiz tool Airflow, dolayısıyla airflow dag’ımızda final_project_dag.py istenen sıralamada çalışacak şekilde task’lerimizi düzenliyoruz.

    [t1,t2] >> t3 >> t4
