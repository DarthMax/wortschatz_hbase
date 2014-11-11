#!/usr/bin/env bash
set -e

export http_proxy="http://proxy.moez.fraunhofer.de:80"
export https_proxy="http://proxy.moez.fraunhofer.de:80"

sudo echo "Acquire::http::proxy \"http://proxy.moez.fraunhofer.de:80\";" >> /etc/apt/apt.conf.d/80proxy
sudo echo "Acquire::ftp::proxy \"http://proxy.moez.fraunhofer.de:80\";" >> /etc/apt/apt.conf.d/80proxy
sudo echo "Acquire::https::proxy \"http://proxy.moez.fraunhofer.de:80\";" >> /etc/apt/apt.conf.d/80proxy

sudo apt-get update
sudo apt-get install python-software-properties software-properties-common --yes
sudo -E add-apt-repository ppa:webupd8team/java --yes
sudo apt-get update


echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections
sudo apt-get install wget oracle-java7-installer --yes

export JAVA_HOME=/usr/lib/jvm/java-7-oracle/

wget http://ftp-stud.hs-esslingen.de/pub/Mirrors/ftp.apache.org/dist/hbase/stable/hbase-0.98.7-hadoop2-bin.tar.gz -O /tmp/hbase.tar.gz

mkdir -p /tmp/hbase
tar -xvf /tmp/hbase.tar.gz -C /tmp/hbase --strip-components=1

sudo mkdir /usr/lib/hbase
sudo mv /tmp/hbase/ /usr/lib/hbase/hbase-0.98.7

echo "export HBASE_HOME=/usr/lib/hbase/hbase-0.98.7" >> ~/.bashrc
echo "export PATH=$PATH:$HBASE_HOME/bin" >> ~/.bashrc

export HBASE_HOME=/usr/lib/hbase/hbase-0.98.7
export PATH=$PATH:$HBASE_HOME/bin

mkdir -p /vagrant/hbase
mkdir -p /vagrant/zookeeper

sudo cat <<HBASECONFIG > /usr/lib/hbase/hbase-0.98.7/conf/hbase-site.xml 
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <value>file:///vagrant/hbase</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/vagrant/zookeeper</value>
  </property>
</configuration>   
HBASECONFIG

start-hbase.sh
 