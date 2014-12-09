mvn clean compile package -DskipTests=true
export Ver="0.1"
export MVNJARS=`find $HOME/.m2/repository/ -name "*.jar" |while read jar; do echo $jar; done |tr '\n' ','`
export LIBJARS="$MVNJARS,$HADOOP_HOME/lib/native"
export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`
export CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`
export HEAPSIZE="10240"
export CMD="hadoop jar ./target/bin2seq-${Ver}.jar" 

case "$1" in
        ppm)
aws s3 rm --recursive s3://ori-bin2seq/colorferet-seq
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.Util -libjars $LIBJARS -in s3://ori-colorferetsubset/00001/ -ext ppm.bz2 -out s3://ori-bin2seq/colorferet-seq -codec snappy 
hadoop fs -rm -f -r /output
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.OpenCV -libjars $LIBJARS s3://ori-bin2seq/colorferet-seq hdfs:///output  
            ;;
         
        tif)
aws s3 rm --recursive s3://ori-bin2seq/landsat-seq
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.Util -libjars $LIBJARS -in s3://nasanex/Landsat/gls/1975/001/026/ -ext tif -out s3://ori-bin2seq/landsat-seq -codec snappy
hadoop fs -rm -f -r /output
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.GeoTiff -libjars ${LIBJARS} s3://ori-bin2seq/landsat-seq hdfs:///output
            ;;
         
        netcdf)
aws s3 rm --recursive s3://ori-bin2seq/netcdf-seq
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.Util -libjars $LIBJARS -in s3://nasanex/NEX-DCP30/BCSD/rcp26/mon/atmos/pr/r1i1p1/v1.0/CONUS/pr_amon_BCSD_rcp26_r1i1p1_CONUS_HadGEM2-ES_200512-200512.nc -ext nc -out s3://ori-bin2seq/netcdf-seq -codec none #large size problem??
hadoop fs -rm -f -r /output
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.NetCDF -libjars ${LIBJARS} s3://ori-bin2seq/netcdf-seq hdfs:///output
            ;;

        hdf5)
aws s3 rm --recursive s3://ori-bin2seq/hdf5-seq
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.Util -libjars $LIBJARS -in s3:///ori-msd10k-h5/data/A/A/A/ -ext h5 -out s3://ori-bin2seq/hdf5-seq -codec snappy
hadoop fs -rm -f -r /output
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.HDF5 -libjars ${LIBJARS} s3://ori-bin2seq/hdf5-seq  /output -attr tempo
            ;;
         
        *)
            echo $"Usage: $0 {ppm|tif|netcdf|hdf5}"
            exit 1
esac

