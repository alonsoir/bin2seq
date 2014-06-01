#!/bin/bash

set -e

# first validate the arguments
REPLACE_FILE=false
for i in "$@" ; do
  case $i in
  --*-heap-size*)
    if ! echo $i | grep -E -- '--[a-zA-Z]+-heap-size=[0-9]+$' > /dev/null 2>&1 ; then
      echo "Couldn't parse option $i expected --cmd-heap-size=1023 where cmd is jobtracker or some such and 1023 is the number of megabytes to allocate to the Java process for that command" 1>&2 
      exit 1
    fi
  ;;
  --*-opts*)
    if ! echo $i | grep -E -- '--[a-zA-Z]+-opts=.+' > /dev/null 2>&1 ; then
      echo "Couldn't parse option $i expected --cmd-opts=-XX:+UseG1GC where cmd is jobtracker or some such and -XX:+UseG1GC is the option to pass to the JVM" 1>&2 
      exit 1
    fi
  ;;
  --help)
    set +x
    echo "Usage: "
    echo "--<daemon>-heap-size"
    echo "   Set the heap size in megabytes for the specified daemon."
    echo " "
    echo "--<daemon>-opts"
    echo "   Set additional Java options for the specified daemon."
    echo " "
    echo "--replace"
    echo "   Replace the existing hadoop-user-env.sh file if it exists."
    echo " "
    echo "<daemon> is one of:"
    echo "  namenode, datanode, jobtracker, tasktracker, client for Hadoop 1.0 or namenode, datanode, resourcemanager, nodemanager, client for Hadoop 2.0"
    echo " "
    echo " "
    echo "Example Usage:"
    echo " --namenode-heap-size=2048 --namenode-opts=\"-XX:GCTimeRatio=19\""
    exit 1
  ;;
  --replace)
    REPLACE_FILE=true
  ;;
  *)
    echo "Unknown option $i" 1>&2
    exit 1
  ;;
  esac
done

set -x

HADOOP_ENV_FILE=/home/hadoop/conf/hadoop-user-env.sh

if [ $REPLACE_FILE == "true" ] ; then
  rm -rf $HADOOP_ENV_FILE
fi

if [ -e $HADOOP_ENV_FILE ] ; then
  [[ ! -n $(grep "#\\!/bin/bash" $HADOOP_ENV_FILE ) ]] && echo "#!/bin/bash" >> $HADOOP_ENV_FILE
else
  echo "#!/bin/bash" >> $HADOOP_ENV_FILE
fi

for i in "$@" ; do
  case $i in
  --*-heap-size=*)
    NAME=$(echo $i | sed 's|--\([^-]*\)-heap-size=.*|\1|' | tr 'a-z' 'A-Z')
    if [[ $NAME == "NAMENODE" || $NAME == "DATANODE" || $NAME == "CLIENT" || $NAME == "JOBTRACKER" || $NAME == "TASKTRACKER" ]]; then
      HEAP_SIZE_CMD="HADOOP_${NAME}_HEAPSIZE"
    elif [[ $NAME == "RESOURCEMANAGER" || $NAME == "NODEMANAGER" ]] ; then
      HEAP_SIZE_CMD="YARN_${NAME}_HEAPSIZE"
    else
      echo "Invalid name $NAME"
      exit 1
    fi
    HEAP_SIZE_VALUE=$(echo $i | sed 's|--[^-]*-heap-size=\(.*\)|\1|')
    cat >> $HADOOP_ENV_FILE <<EOF
$HEAP_SIZE_CMD=$HEAP_SIZE_VALUE
EOF
  ;;
  --*-opts*)
    NAME=$(echo $i | sed 's|--\([^-]*\)-opts=.*|\1|' | tr 'a-z' 'A-Z')
    if [[ $NAME == "NAMENODE" || $NAME == "DATANODE" || $NAME == "CLIENT" || $NAME == "JOBTRACKER" || $NAME == "TASKTRACKER" ]]; then
      OPTS_CMD="HADOOP_${NAME}_OPTS"
    elif [[ $NAME == "RESOURCEMANAGER" || $NAME == "NODEMANAGER" ]] ; then
      OPTS_CMD="YARN_${NAME}_OPTS"
    else
      echo "Invalid name $NAME"
      exit 1
    fi
    OPTS_VALUE=$(echo $i | sed 's|--[^-]*-opts=\(.*\)|\1|')
    cat >> $HADOOP_ENV_FILE <<EOF
$OPTS_CMD="$OPTS_VALUE"
EOF
  ;;
  esac
done

cd /home/hadoop

wget --no-check-certificate https://javacv.googlecode.com/files/javacv-0.7-bin.zip
unzip javacv-0.7-bin.zip

wget --no-check-certificate  https://javacv.googlecode.com/files/javacv-0.7-cppjars.zip
unzip javacv-0.7-cppjars.zip

#ideally
#rpm -Uvh  http://mirror.centos.org/centos/6/os/x86_64/Packages/centos-release-6-5.el6.centos.11.1.x86_64.rpm
#but does not work for EMR instance, as a workaround:
sudo wget --no-check-certificate https://raw.githubusercontent.com/dayne/yum/master/centos6/CentOS-Base.repo -O /etc/yum.repos.d/CentOS-Base.repo
sudo sed -i 's/$releasever/6/g' /etc/yum.repos.d/CentOS-Base.repo
sudo yum -y install gtk2 libv4l
sudo yum -y --enablerepo epel install hdf5
sudo rpm -i http://dl.fedoraproject.org/pub/epel/6/x86_64/hdf5-devel-1.8.5.patch1-7.el6.x86_64.rpm
sudo rpm -i ftp://ftp.pbone.net/mirror/archive.fedoraproject.org/fedora/linux/updates/15/x86_64/jhdf5-2.7-5.fc15.x86_64.rpm

echo "" >>~/.bashrc
echo "export LD_LIBRARY_PATH=/usr/lib64/jhdf5:$LD_LIBRARY_PATH" >> ~/.bashrc
echo "export JAVACV_BIN_HOME=/home/hadoop/javacv-bin" >>~/.bashrc
echo "export JAVACV_CPP_HOME=/home/hadoop/javacv-cppjars"  >>~/.bashrc
echo "export AWS_ACCESS_KEY=" >>~/.bashrc
echo "export AWS_SECRET_KEY=" >>~/.bashrc

#only on NN, no need to bootstrap to all nodes
#sudo yum -y --enablerepo epel install s3cmd
#sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
#sudo sed -i 's/$releasever/6/g' /etc/yum.repos.d/epel-apache-maven.repo
#sudo yum -y install apache-maven git

