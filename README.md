# K-Means in Spark
K-Means implementation to cluster documents from the [StackExchange Dataset](https://archive.org/details/stackexchange).

Code authors
-----------
 - Nithin Gangadharan
 - Pranav Sharma

Installation
------------
These components are installed:
- JDK 1.8 or JDK 11
- Scala 2.11.12 or Scala 2.12.10
- Hadoop 2.9.1 or 3.2.2
- Spark 2.3.1 or 3.0.1 (without bundled Hadoop)
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example ~/.bash_aliases:

- `export JAVA_HOME=/usr/lib/jvm/java-8-oracle`
- `export HADOOP_HOME=/home/joe/tools/hadoop/hadoop-2.9.1`
- `export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop`
- `export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin`

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
   export JAVA_HOME=/usr/lib/jvm/java-8-oracle

3) Make a copy of the `.env.example` file into a file called `.env`(in the same location) and then fill in the values. This is so that we share the same makefile for different projects, and work independently, without needing to commit makefile changes into the repo (.env is git-ignored).

Execution
---------
All of the build & execution commands are organized in the Makefile.

1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top. Sufficient for standalone:
   hadoop.root, jar.name, local.input Other defaults acceptable for running standalone.
5) Standalone Hadoop:
	- `make switch-standalone`        -- set standalone Hadoop environment (execute once)
	- `make seq-local`                -- run **sequential k-means** locally
	- `make distributed-local`        -- run **distributed k-means** locally
6) Pseudo-Distributed
   Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	- `make switch-pseudo`             -- set pseudo-clustered Hadoop environment (execute once)
	- `make pseudo`                    -- first execution
	- `make pseudoq`                   -- later executions since namenode and datanode already running
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	- `make upload-input-aws`         -- only before first execution
	- `make aws`                      -- check for successful **k-means sequential** execution with web interface (aws.amazon.com)
	- `make aws-distributed`          -- check for successful **k-means distributed** execution with web interface (aws.amazon.com)
	- `download-output-aws`           -- after successful execution & termination
	
Notes
----------
1. You may need to set certain variables in a .env file. The instructions to setup this file are mentioned in the 3rd point under the *Environment* section.
2. The parsing logic of input data is specific to the the *Posts* xml files found in the stackexchange dataset. The program will not work with other kind of data.
3. Make sure that the different posts.xml have a unique-name since it'll be used as an id in the code. The ids in the xml files always start from 1 (they are not universal), 
   so this helps the program identify different entries.
4. Make sure that the posts.xml don't contain any BOM characters. Use a [tool like this](https://github.com/alastairruhm/utfbom-remove) to remove them. 
   Alternatively, you may also use the vim editor to remove BOM characters. To do this open file in vim editor, use `:set nobomb` and save & quit file using `:wq`. 
