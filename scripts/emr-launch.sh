
export bootstrap="s3://ori-emrscript/bin2seq-emr-bootstrap.sh"
#export args="-m,mapred.child.java.opts=-Xmx4096m"
export args="-namenode-heap-size=6144,--jobtracker-heap-size=6144,--tasktracker-heap-size=6144,--namenode-opts=-XX:+UseCompressedOops,--jobtracker-opts=-XX:+UseCompressedOops,--tasktracker-opts=-XX:+UseCompressedOops"
export clustername="test"
export masterinstance="m3.xlarge"
export slaveinstance="m3.xlarge"
export slaves="1"
export bidprice="0.04"

elastic-mapreduce --create --bootstrap-action $bootstrap --args $args --name $clustername --alive --hadoop-version 2.2.0 --ami-version latest --instance-group master --instance-type $masterinstance --instance-count 1 --bid-price $bidprice --instance-group core --instance-type $slaveinstance --instance-count $slaves  --bid-price $bidprice

export jobid=`elastic-mapreduce --list --active |awk '{print $1}'`
elastic-mapreduce --jobflow $jobid --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args '--src,s3://ori-msd10k-seq/,--dest,hdfs:///msd10k'
elastic-mapreduce --jobflow $jobid --jar /home/hadoop/lib/emr-s3distcp-1.0.jar --args '--src,s3://ori-colorferet-seq/,--dest,hdfs:///colorferet'

