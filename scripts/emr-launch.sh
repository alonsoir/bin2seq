
export bootstrap="s3://ori-emrscript/bin2seq-emr-bootstrap.sh"
export args="-m,mapred.child.java.opts=-Xmx4096m"
export clustername="test"
export masterinstance="m3.xlarge"
export slaveinstance="m3.xlarge"
export slaves="1"
export bidprice="0.04"

elastic-mapreduce --create --bootstrap-action $bootstrap --args args --name $clustername --alive --hadoop-version 2.2.0 --ami-version latest --instance-group master --instance-type $masterinstance --instance-count 1 --bid-price $bidprice --instance-group core --instance-type $slaveinstance --instance-count $slaves  --bid-price $bidprice
