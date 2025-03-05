# batch-processing

## Setting up the environment on GCP cloud VM(Ubuntu 20.04)
1. Generating SSH keys
2. Creating a virtual machine on GCP
3. Connecting to the VM with SSH
4. Installing Anaconda
      ```
       wget -P /tmp https://repo.anaconda.com/archive/Anaconda3-2020.02-Linux-x86_64.sh
      sha256sum /tmp/Anaconda3-2020.02-Linux-x86_64.sh
      bash /tmp/Anaconda3-2020.02-Linux-x86_64.sh
      source ~/.bashrc (path added to .bashrc)
      nano .bashrc
     ```
5. Installing Docker

    ```
    sudo apt-get update
    sudo apt-get install docker.io
    docker --version
    docker run hello-world (it gave permission denied)
    sudo groupadd docker
    sudo usermod -aG docker $USER
    logout
    ssh de-vm2025
    docker run hello-world
    ```

6. Installing docker-compose

    ```
     mkdir bin
    cd bin/
     wget https://github.com/docker/compose/releases/download/v2.2.3/docker-compose-linux-x86_64 -O docker-compose
      ls
      chmod +x docker-compose
      ls
      ./docker-compose
      ./docker-compose version
       cd ..
      nano .bashrc
      source .bashrc
     which docker-compose
    docker-compose version
    ```

7. Creating SSH config file

    ```
    created config file at .ssh/ directory already having gcp public and private keys
    configured as follow:
    Host de-vm2025
        HostName 34.42.82.230
        User suman
        IdentityFile C:/users/satin/.ssh/gcp
    ```

8. Accessing the remote machine with VS Code and SSH remote

    ```
    remote ssh plugin installed on VS-code
    ssh into VM (connect to host)
    ```

## Installing Java

Download OpenJDK 11 or Oracle JDK 11 (It's important that the version is 11 - spark requires 8 or 11)

We'll use [OpenJDK](https://jdk.java.net/archive/)

Download it (e.g. to `~/spark`):mkdir spark

```
wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
```

Unpack it:

```bash
tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz
```

define `JAVA_HOME` and add it to `PATH`:

```bash
export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"
```

check that it works:

```bash
java --version
```

Output:

```
openjdk 11.0.2 2019-01-15
OpenJDK Runtime Environment 18.9 (build 11.0.2+9)
OpenJDK 64-Bit Server VM 18.9 (build 11.0.2+9, mixed mode)
```
Remove the archive:

```bash
rm openjdk-11.0.2_linux-x64_bin.tar.gz
```
### Installing Spark


Download Spark. Use 3.3.2 version:

```bash
wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
```

Unpack:

```bash
tar xzfv spark-3.3.2-bin-hadoop3.tgz
```

Remove the archive:

```bash
rm spark-3.3.2-bin-hadoop3.tgz
```

Add it to `PATH`:

```bash
export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
```
### Testing Spark

Execute `spark-shell` and run the following:

```scala
val data = 1 to 10000
val distData = sc.parallelize(data)
distData.filter(_ < 10).collect()
```
## PySpark

To run PySpark, we first need to add it to `PYTHONPATH`:

```bash
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
```

Make sure that the version under `${SPARK_HOME}/python/lib/` matches the filename of py4j or you will
encounter `ModuleNotFoundError: No module named 'py4j'` while executing `import pyspark`.

For example, if the file under `${SPARK_HOME}/python/lib/` is `py4j-0.10.9.3-src.zip`, then the
`export PYTHONPATH` statement above should be changed to

```bash
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.3-src.zip:$PYTHONPATH"
```

On Windows, you may have to do path conversion from unix-style to windows-style:

```bash
SPARK_WIN=`cygpath -w ${SPARK_HOME}`

export PYTHONPATH="${SPARK_WIN}\\python\\"
export PYTHONPATH="${SPARK_WIN}\\python\\lib\\py4j-0.10.9-src.zip;$PYTHONPATH"
```

Now you can run Jupyter or IPython to test if things work. Go to some other directory, e.g. `~/batch-processing`.

Download a CSV file that we'll use for testing:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Now let's run `ipython` (or `jupyter notebook`) and execute:

```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv('taxi_zone_lookup.csv')

df.show()
```

Test that writing works as well:

```python
df.write.parquet('zones')
```
- Reading Data in Spark
  
    ```
    df = spark.read \
        .option("header", "true") \
        .csv('fhvhv_tripdata_2021-01.csv')
    ```
- Reading as Panda dataframe
 
      ```
      df_pandas = pd.read_csv('fhvhv_tripdata_2021-01.csv')
      ```
- Reading schema
 
      ```
      spark.createDataFrame(df_pandas).schema
      ```
- creating schema
 
      ```
        schema = types.StructType([
        types.StructField('hvfhs_license_num', types.StringType(), True),
        types.StructField('dispatching_base_num', types.StringType(), True),
        types.StructField('pickup_datetime', types.TimestampType(), True),
        types.StructField('dropoff_datetime', types.TimestampType(), True),
        types.StructField('PULocationID', types.IntegerType(), True),
        types.StructField('DOLocationID', types.IntegerType(), True),
        types.StructField('SR_Flag', types.StringType(), True)
        ])
        ```
- Reading data in spark with defined schema
  
    ```
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv('fhvhv_tripdata_2021-01.csv')
    ```
- Partition/Repartition
  
    ```
    df = df.repartition(24) # created 24 partitions
    ```
- write data to parquet
  
    ```
    df.write.parquet('fhvhv/2021/01/', mode='overwrite')
    df.coalesce(1).write.parquet('data/report/revenue/', mode='overwrite') 
    
    df_green_revenue \
        .repartition(20) \
        .write.parquet('data/report/revenue/green', mode='overwrite')
    ```
- wrting spark sql
  
    ```
    import library
    from pyspark.sql import functions as F
    ```
- Renaming and cretaed new columns
  
    ```
    df \
        .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
        .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
        .withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num)) \
        .select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
        .show()
    ```
- Select and filter
  
    ```
    df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID') \
      .filter(df.hvfhs_license_num == 'HV0003')
    ```
- adding new values with contant value using lit function (create a column with a literal value)
  
    ```
    df_green_sel = df_green \
        .select(common_colums) \
        .withColumn('service_type', F.lit('green'))
    
    df_yellow_sel = df_yellow \
        .select(common_colums) \
        .withColumn('service_type', F.lit('yellow'))
    ```
- Union or merge data
  
    ```
    df_trips_data = df_green_sel.unionAll(df_yellow_sel)
    ```
- Group by
  
    ```
    df_trips_data.groupBy('service_type').count().show()
    ```
- creating temp table for sql queries
  
    ```
    df_trips_data.registerTempTable('trips_data')
    
    spark.sql("""
    SELECT
        service_type,
        count(1)
    FROM
        trips_data
    GROUP BY 
        service_type
    """).show()
    ```
