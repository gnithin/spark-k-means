# Makefile for Spark WordCount project.
include .env

# Customize these pats for your environment.
# -----------------------------------------------------------
spark.root=$(PRIVATE_SPARK_PATH)
hadoop.root=$(PRIVATE_HADOOP_ROOT)
app.name=K Means
jar.name=kmeans.jar
maven.jar.name=kmeans-1.0.jar
job.sequential.name=sequential.KMeansSequential
job.name=$(PRIVATE_AWS_JOB_NAME)
job.distributed.name=distributed.KMeansDistributed
local.master=local[4]
local.distributed.input_centers=distributed_input/centers.txt
local.distributed.input_data=distributed_input/data
local.distributed.output=distributed_output
local.sequential.input=sequential_input
local.sequential.output=sequential_output
# Pseudo-Cluster Execution
hdfs.user.name=joe
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
# aws.emr.release=emr-5.17.0
aws.emr.release=emr-6.2.0
aws.bucket.name=$(PRIVATE_AWS_BUCKET_NAME)
aws.input=$(PRIVATE_AWS_RUN_INPUT)
aws.distributed.input_data=$(PRIVATE_AWS_RUN_INPUT_DATA)
aws.distributed.input_centers=distributed_input/centers.txt
aws.output=$(PRIVATE_AWS_RUN_OUTPUT)
aws.log.dir=log
aws.num.nodes=8
aws.master=yarn
aws.instance.type=m4.large
local.input=$(PRIVATE_AWS_RUN_INPUT)
local.output=$(PRIVATE_AWS_RUN_OUTPUT)
local.log=log
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package
	cp target/${maven.jar.name} ${jar.name}

# Removes local output directory.
clean-local-output:
	rm -rf ${local.distributed.output}*
	rm -rf ${local.sequential.output}*

# Runs standalone
local: jar clean-local-output
	spark-submit --class ${job.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.output}

# Runs standalone kmeans sequential
seq-local: jar clean-local-output
	spark-submit --class ${job.sequential.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.sequential.input} ${local.sequential.output}

# Runs standalone kmeans distributed
distributed-local: jar clean-local-output
	spark-submit --class ${job.distributed.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.distributed.input_centers} ${local.distributed.input_data} ${local.distributed.output} ${local.master}


# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs: 
	${hadoop.root}/sbin/stop-dfs.sh
	
# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.	
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output-hdfs:
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	spark-submit --class ${job.name} --master yarn --deploy-mode cluster ${jar.name} ${local.input} ${local.output}
	make download-output-hdfs

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	spark-submit --class ${job.name} --master yarn --deploy-mode cluster ${jar.name} ${local.input} ${local.output}
	make download-output-hdfs

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.name} s3://${aws.bucket.name}

# Main EMR launch.
aws: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "K Means" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate

# Main EMR launch.
aws-distributed: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "K Means Distributed" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.distributed.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.distributed.input_centers}","s3://${aws.bucket.name}/${aws.distributed.input_data}","s3://${aws.bucket.name}/${aws.output}","${aws.master}"] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate


# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output}
	mkdir ${local.log}
	aws s3 sync s3://${aws.bucket.name}/${aws.log.dir} ${local.log}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -f Spark-Demo.tar.gz
	rm -f Spark-Demo.zip
	rm -rf build
	mkdir -p build/deliv/Spark-Demo
	cp -r src build/deliv/Spark-Demo
	cp -r config build/deliv/Spark-Demo
	cp -r input build/deliv/Spark-Demo
	cp pom.xml build/deliv/Spark-Demo
	cp Makefile build/deliv/Spark-Demo
	cp README.txt build/deliv/Spark-Demo
	tar -czf Spark-Demo.tar.gz -C build/deliv Spark-Demo
	cd build/deliv && zip -rq ../../Spark-Demo.zip Spark-Demo
