#!/bin/sh

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
#sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
#sudo sed -i 's/$releasever/6/g' /etc/yum.repos.d/epel-apache-maven.repo
#sudo yum -y install apache-maven git

#To compare with serial 
#sudo yum -y install numpy opencv* python-imaging 

