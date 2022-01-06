# spark-log_parser
A Demo Project to read and parse log file data using spark.

This readme file contains the instruction to set up and run the application in local machine.


## Prerequisites:
* Git
* Maven
* IntelliJ


### spark-log_parser setup:

* Clone the repository using below command
```shell
git clone https://github.com/krgauraw/eict-project-data_engineering.git
```
* Import/Open Project (eict-project-data_engineering) into IntelliJ
* Open Module Setting of spark-log_parser and change/configure Project SDK under "Project Setting" to 1.8. Refer to below sample image: 
![project-setting-image](images/project-setting.png?raw=true "Project Settings")
* Open a Terminal from project location (eict-project-data_engineering) and do the build using command:
```shell
mvn clean install -DskipTests
```
* Copy Sample Data (located under data folder) to any location in the system. e.g: /data/spark/project/NASA_access_log_Aug95.gz
* Create an Application (e.g: Driver Program) and provide required program arguments. Sample Image is as below:
![application-config-image](images/application.png?raw=true "Application Configuration")
* Run the application. output will be printed in console.