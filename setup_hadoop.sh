wget http://artfiles.org/apache.org/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz
tar xfz hadoop-2.6.0.tar.gz 
mv hadoop-2.6.0 hadoop

vim .bashrc
##HADOOP VARIABLES START
#export JAVA_HOME=/usr/lib/jvm/java/
#export HADOOP_INSTALL=/disk/localhome/hadoop/hadoop
#export PATH=$PATH:$HADOOP_INSTALL/bin
#export PATH=$PATH:$HADOOP_INSTALL/sbin
#export HADOOP_MAPRED_HOME=$HADOOP_INSTALL
#export HADOOP_COMMON_HOME=$HADOOP_INSTALL
#export HADOOP_HDFS_HOME=$HADOOP_INSTALL
#export YARN_HOME=$HADOOP_INSTALL
#export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_INSTALL/lib/native
#export HADOOP_OPTS="-Djava.library.path=$HADOOP_INSTALL/lib"
##HADOOP VARIABLES END

source ~/.bashrc

cd hadoop

vim etc/hadoop/hadoop-env.sh
#export JAVA_HOME=/usr/lib/jvm/java/

vim etc/hadoop/core-site.xml
#<configuration>
#        <property>
#                <name>fs.default.name</name>
#                <value>hdfs://nemo.tm.informatik.uni-leipzig.de:54310</value>
#                <description>The name of the default file system.  A URI whose
#                scheme and authority determine the FileSystem implementation.  The
#                uri's scheme determines the config property (fs.SCHEME.impl) naming
#                the FileSystem implementation class.  The uri's authority is used to
#                determine the host, port, etc. for a filesystem.</description>
#        </property>
#
#        <property>
#                <name>hadoop.tmp.dir</name>
#                <value>/disk/data/hadoop/tmp</value>
#                <description>A base for other temporary directories.</description>
#        </property>
#</configuration>

vim etc/hadoop/yarn-site.xml
#<configuration>
#        <property>
#                <name>yarn.nodemanager.aux-services</name>
#                <value>mapreduce_shuffle</value>
#        </property>
#        <property>
#                <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
#                <value>org.apache.hadoop.mapred.ShuffleHandler</value>
#        </property>
#</configuration>

cp etc/hadoop/mapred-site.xml.template etc/hadoop/mapred-site.xml
vim etc/hadoop/mapred-site.xml
#<configuration>
#        <property>
#                <name>mapreduce.framework.name</name>
#                <value>yarn</value>
#        </property>
#</configuration>

mkdir -p /disk/data/hadoop/hdfs/namenode
mkdir -p /disk/data/hadoop/hdfs/datanode
mkdir -p /disk/data/hadoop/tmp
vim etc/hadoop/hdfs-site.xml
#<configuration>
#        <property>
#                <name>dfs.replication</name>
#                <value>1</value>
#        </property>
#        <property>
#                <name>dfs.namenode.name.dir</name>
#                <value>file:/disk/data/hadoop/hdfs/namenode</value>
#        </property>
#        <property>
#                <name>dfs.datanode.data.dir</name>
#                <value>file:/disk/data/hadoop/hdfs/datanode</value>
#        </property>
#</configuration>


#(NUR MASTER (nemo))
vim etc/hadoop/slaves
#nemo.tm.informatik.uni-leipzig.de
#arielle.tm.informatik.uni-leipzig.de
#atlantis.tm.informatik.uni-leipzig.de