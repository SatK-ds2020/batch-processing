# batch-processing

## Setting up the environment on GCP cloud VM(Ubuntu 20.04)
- Generating SSH keys
- Creating a virtual machine on GCP
- Connecting to the VM with SSH
- Installing Anaconda
- Installing Docker
- Creating SSH config file
- Accessing the remote machine with VS Code and SSH remote

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



  1  ls
    2  sudo su
    3  mkdir spark
    4  ls
    5  cd spark/
2. Java Installed    
    6  wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
    7  tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz
    8  rm openjdk-11.0.2_linux-x64_bin.tar.gz
    9  ls
   10  pwd
3. Java Home set up
   11  export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
   12  export PATH="${JAVA_HOME}/bin:${PATH}"
   13  java --version
   14  which java
4. Spark Installed
   15  wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
   16  tar xzfv spark-3.3.2-bin-hadoop3.tgz
   17  rm spark-3.3.2-bin-hadoop3.tgz
5. Spark Home set up
   18  export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
   19  export PATH="${SPARK_HOME}/bin:${PATH}"
   20  spark-shell
   21  cd ..
   22  nano .bashrc
6. Project directory created
   23  mkdir notebooks
   24  cd notebooks/
   25  jupyter notebook
   26  cd ..
   27  wget -P /tmp https://repo.anaconda.com/archive/Anaconda3-2020.02-Linux-x86_64.sh
   28  sha256sum /tmp/Anaconda3-2020.02-Linux-x86_64.sh
   29  bash /tmp/Anaconda3-2020.02-Linux-x86_64.sh
   30  source ~/.bashrc
   31  nano .bashrc
   32  ls
   33  logout
   34  which java
   35  which pyspark
   36  ls
   37  cd notebooks
   38  jupyter notebook
   39  cd ..
7. Python Path added as system variable
   40  export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
   41  export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
   42  which python
   43  cd notebook
   44  ls
   45  cd notebooks
   46  jupyter notebook
   47  cd ..
   48  history
