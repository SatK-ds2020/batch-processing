# batch-processing

1. Google VM-Ubuntu was created
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