export emrbootstrap="s3://elasticmapreduce/bootstrap-actions/configure-daemons"
export appbootstrap="s3://ori-emrscript/bin2seq-emr-bootstrap.sh"
export emrlog="s3://ori-tmp/emr-log"
export keypair="gsg-keypair"
#export args="--replace --namenode-heap-size=10240,--datanode-heap-size=10240,--client-heap-size=10240,--namenode-opts=-XX:+UseCompressedOops,--jobtracker-opts=-XX:+UseCompressedOops,--tasktracker-opts=-XX:+UseCompressedOops, -m,mapred.child.java.opts=-Xmx10240"
export args="--namenode-heap-size=10240,--datanode-heap-size=10240,--client-heap-size=10240,--namenode-opts=-XX:+UseCompressedOops"
export clustername="test"
export amiversion="3.3.1"
export masterinstance="m3.xlarge"
export slaveinstance="m3.xlarge"
export slaves="2"
export bidprice=`aws ec2 describe-spot-price-history --instance-types m3.xlarge --product-descriptions Linux/UNIX |grep SpotPrice |sort |head -n 1 |cut -d':' -f2 | sed -e 's/"//g' -e 's/,//g' |awk '{print $1 + 0.01}'  |cut -c1-4` #lowest bid + $0.01
echo "biding price="$bidprice

#clean up log from previous runs
aws s3 rm --recursive $emrlog

#create EMR cluster
aws emr create-cluster --enable-debugging --log-uri $emrlog  --ec2-attributes KeyName=$keypair --ami-version $amiversion \
--bootstrap-actions Path=$emrbootstrap,Name="Set VM parameters",Args=[$args] \
Path=$appbootstrap,Name="Install dependencies" \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,BidPrice=$bidprice,Name=Master,InstanceType=$masterinstance InstanceGroupType=CORE,BidPrice=$bidprice,Name=Slave,InstanceCount=$slaves,InstanceType=$slaveinstance 

#elastic-mapreduce --jobflow $jobid --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args '--src,s3://ori-msd10k-seq/,--dest,hdfs:///msd10k'
#elastic-mapreduce --jobflow $jobid --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args '--src,s3://ori-colorferet-seq/,--dest,hdfs:///colorferet'

