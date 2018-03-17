<!--
Title: Bluevigil
Description:  Bluevigil security analytics application

-->
# Bluevigil

Bluevigil is an entreprise ready network security analysis application which provides security analytics and insights on a huge volume of data on real time or near real time.
This is a streaming analytics application that makes it faster and easier to make decisions for network analysts and security operations personnals.

This provides organizations to ingest, enrich, store and process the different types of network logs and other security related data feeds in a scalable and distributed way 
in order to detect security threats and anomalies in real time and respond to it rapidly. 

This application provides below capabilities.

Security data vault
Platform provides cost effective way to store raw and enriched network security data for a long period of time with inherint fault tolerance and scalability.

Data discovery
Platform provides ability to discover the required data from a huge volume of raw and enriched data with simple and friendly user interfaces. Network analysts can define and configure different 
filters based on their requirements to get the desired data from historical and real time data. 

Advanced Analytics
This application provides ability to do advanced querying and analysis based on the standard SQL statements on historical and real time data.

API interfaces
This platform provides different API end points for accessing the data available in security data vault.

# Requirements
Apache Maven 3.3.3 or higher
Java 1.8 or higher
Apache Spark 1.6
Apach Hadoop 2.7.5
Apache Oozie 4.2.1

# How to build 
Clone the project from this repository
Go to your project path 
Run below command to generate all required Jars and War 
mvn -DskipTests clean package
This will generate jar and war files required for deploying Bluevigil application

bluevigil-streaming-core-jar-with-dependencies.jar
bluevigil-streaming-core.jar
BluevigilAnalyticsServices.war

#Installation

#Step 1: 
    Configure flume - Create flume config file for each type of network file available in the networks logs directory.
        Edit the sample flume config template available at conf/flume_config_templates/.
        Modify the networks logs directory - Spool directory
        Modify file pattern to include the specific files from the directory
        Modify input Kafka topic name for this type of file.
        Add Kafka broker list according to system configuration.
#Step 2: 
    Configure Oozie workflow.xml and job.properties file
		Modify the workflow.xml and job.properties file available at conf/workflow_templates.
		Arg0: source topic name - input kafka topic name mentioned in previous step.
		Arg1: Output Kafka topic name where the processed data pushed to consume by Web UI.
		Arg2: Bootstrap server details.
		Arg3: Zookeeper server details.
		Arg4: Network log json file path - Json property file for different types of log files available at conf/properties.
#Step 3: 
    Create required tables in Phoenix by executing script createTable.sh at conf/scripts
		sh createTable.sh
#Step 4: 
    Execute oozie workflow from the jar path
		oozie job -oozie http://<Your Ip>:11000/oozie -config job.properties -run
#Step 5:
    Deploy BluevigilAnalyticsServices.war