wget http://mirror.serversupportforum.de/apache/hbase/stable/hbase-1.0.0-bin.tar.gz
tar xvzf hbase-1.0.0-bin.tar.gz
mv hbase-1.0.0 hbase

cd hbase

vim conf/hbase-env.sh
#export JAVA_HOME=/usr/lib/jvm/java/


vim conf/hbase-site.xml
#<configuration>
#        <property>
#                <name>hbase.rootdir</name>
#                <value>hdfs://nemo.tm.informatik.uni-leipzig.de:9000/hbase</value>
#        </property>
#        <property>
#                <name>hbase.zookeeper.property.dataDir</name>
#                <value>/disk/data/hadoop/zookeeper</value>
#        </property>
#        <property>
#                <name>hbase.cluster.distributed</name>
#                <value>true</value>
#        </property>
#        <property>
#                <name>hbase.zookeeper.quorum</name>
#                <value>arielle.tm.informatik.uni-leipzig.de,nemo.tm.informatik.uni-leipzig.de,atlantis.tm.informatik.uni-leipzig.de</value>
#        </property>
#</configuration>


vim conf/regionservers
#atlantis.tm.informatik.uni-leipzig.de
#nemo.tm.informatik.uni-leipzig.de






