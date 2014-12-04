export emrbootstrap="s3://elasticmapreduce/bootstrap-actions/configure-daemons"
export hadoopbootstrap="s3://elasticmapreduce/bootstrap-actions/configure-hadoop"
export appbootstrap="s3://ori-emrscript/bin2seq-emr-bootstrap.sh"
export emrlog="s3://ori-tmp/emr-log/"
export keypair="gsg-keypair"
#export args="--replace --namenode-heap-size=10240,--datanode-heap-size=10240,--client-heap-size=10240,--namenode-opts=-XX:+UseCompressedOops,--jobtracker-opts=-XX:+UseCompressedOops,--tasktracker-opts=-XX:+UseCompressedOops, -m,mapred.child.java.opts=-Xmx10240"
export args="--namenode-heap-size=4096,--datanode-heap-size=4096,--client-heap-size=4096,--namenode-opts=-XX:+UseCompressedOops,--client-opts=mapred.map.child.java.opts=-Xmx4G"
export clustername="bin2seq"
export amiversion="3.3.1"
export instance="r3.xlarge" #"m3.xlarge"
export cores="1"
export tasks="1"

#clean up log from previous runs
aws s3 rm --recursive $emrlog

#Query the cheapest price for EMR instance
export bidprice=`aws ec2 describe-spot-price-history --instance-types $instance --product-descriptions Linux/UNIX |grep SpotPrice |sort |head -n 1 |cut -d':' -f2 | sed -e 's/"//g' -e 's/,//g' |awk '{print $1 + 0.01}'  |cut -c1-4` #lowest bid + $0.01
echo "biding price="$bidprice
#create EMR cluster
aws emr create-cluster --name $clustername --enable-debugging --log-uri $emrlog  --ec2-attributes KeyName=$keypair --ami-version $amiversion \
--bootstrap-actions Path=$emrbootstrap,Name="Set VM parameters",Args=[$args] \
--bootstrap-actions Path=$hadoopbootstrap,Name="Set Hadoop paramters",Args=["-m,mapreduce.map.java.opts=-Xmx4G"] \
Path=$appbootstrap,Name="Install dependencies" \
--instance-groups \
Name=Master,InstanceGroupType=MASTER,InstanceCount=1,BidPrice=$bidprice,Name=Master,InstanceType=$instance \
Name=Core,InstanceGroupType=CORE,BidPrice=$bidprice,InstanceCount=$cores,InstanceType=$instance \
Name=Task,InstanceGroupType=TASK,BidPrice=$bidprice,InstanceCount=$tasks,InstanceType=$instance 


#Path=$emrbootstrap,Name="Set Client Opts",Args=[$opts] \
#elastic-mapreduce --jobflow $jobid --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args '--src,s3://ori-msd10k-seq/,--dest,hdfs:///msd10k'
#elastic-mapreduce --jobflow $jobid --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args '--src,s3://ori-colorferet-seq/,--dest,hdfs:///colorferet'

