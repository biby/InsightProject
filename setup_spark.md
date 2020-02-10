<h1>
Installing a Spark cluster
</h1>

I followed this awsome <a href="https://blog.insightdatascience.com/simply-install-spark-cluster-mode-341843a52b88"> tutorial </a>, changing versions when needed.

<h1>
My subnet
</h1>

<p>All my instances run on a Ubuntu 18.04 server. My spark clusted has one master and 4 worker nodes with following IPs.</p>
<ul>
<li>Master: `10.0.0.10`</li>
<li>Workers: `10.0.0.4`,`10.0.0.9`,`10.0.0.11`,`10.0.0.13`</li>
</ul>

<h1>
Spark install
</h1>
<p> On each instance, install java 8, scala. Then  download and unzip spark 2.5.0</p>

```bash
sudo apt update
sudo apt-get install openjdk-8-jre-headless
sudo apt-get install scala
wget http://apache.claz.org/spark/spark-2.5.0/spark-2.5.0-bin-hadoop2.7.tgz
tar xvf spark-2.5.0-bin-hadoop2.7.tgz
```
<p> Move your spark to '/usr/local/spark' and set up your bash_profile.</p>

```bash
sudo mv spark-2.5.0-bin-hadoop2.7/ /usr/local/spark
echo 'export PATH=/usr/local/spark/bin:$PATH' >> ~/.bash_profile
source ~/.bash_profile
```

<p> Give the workers IPs to the master</p>

```bash
echo "10.0.0.4\n10.0.0.9\n10.0.0.11\n10.0.0.13\n" > /usr/local/spark/conf/slaves
```

<p> Provide master node IP and java location to spark's environment:</p>
```bash
cp /usr/local/spark/conf/spark-env.sh.template /usr/local/spark/conf/spark-env.sh 
echo "export SPARK_MASTER_HOST=<master-private-ip>\nexport JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/" >>/usr/local/spark/conf/spark-env.sh
```

<h2> Allow keyless comunication from the master to the workers</h2>
<p>Generate an ssh key on your master.</p>
```bash
sudo apt-get install openssh-server
cd ~/ssh
ssh-keygen -t rsa -P ""
```

<p>Add the content of `~/.ssh/id_rsa.pub` to the `~/.ssh/authorized_keys` of all of the workers<p>

<p>Define your AWS security groups to allow the master to comunicate to the workers</p>

<h1>Start spark</h1>

```bash
sbin/start-all.sh
```


<h1> Configure a virtual environement for spark </h1>

I followed <a href="https://community.cloudera.com/t5/Community-Articles/Using-VirtualEnv-with-PySpark/ta-p/245905">this tutorial</a>

Install virtualenv:
```bash
sudo apt-get install virtualenv
```
Create a virtual environement with python3
```bash
virtualenv sparkenv -p /usr/local/bin/python3
```

Activate it, install the required libraries, and save the requirements in  requirements.txt file 
```bash
source sparkenv/bin/activate  # activate virtualenv
pip install numpy pillow boto3
pip freeze > requirements.txt
```

You should obtain a file similar to <a href="/src/spark/requirements.txt">this one</a>

<h1>Set up the spark configuration file</h1>
<p>Copy the <a href="/src/spark/spark-defaults.conf">spark-defaults.conf</a> file to /usr/local/spark/conf.</p>
<p>This file contains the java packages for using s3 as well as the <a href="https://github.com/streamnative/pulsar-spark">pyspark-pulsar connector</a>. It also defines spark master's IP and port to be 10.0.0.10:7077, and allocates 6g of memory per executor</p>


<p>You can start spark with the following command:</p>
```bash
spark-submit --master spark://10.0.0.10:7077 --conf spark.pyspark.virtualenv.enabled=true  --conf spark.pyspark.virtualenv.type=native --conf spark.pyspark.virtualenv.requirements=/PATH/TO/REQUIREMENTS/requirements.txt --conf spark.pyspark.virtualenv.bin.path=/PATH/TO/BIN/VIRTUALENV/FOLDER/ --conf spark.pyspark.python=/PATH/TU/PYTHON3/python3 ./src/spark/SolarInsight.py
```



