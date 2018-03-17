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
Apach Hadoop 
Apache Oozie 3.0.0

# How to build 
Clone the project from this repository
Go to your project path 
Run below command to generate all required Jars and War 
mvn -DskipTests clean package

# Preprequisite
Modify the initial_config file to reflect your property file details
Execute Phoenix script to create all required tables 
Execute the provided oozie workflow.

bluevigil_property_file_path

# How to deploy
mvn -DskipTests clean package