
## Phase 0 - Initial setup

To install all the required libraries and dependencies, run the following command in your [virtual environment](https://docs.python.org/3/library/venv.html):

* <code>pip3 install -r requirements.txt</code>

## Phase 1 - Setting up training dataset 

* Download the initial Amazon dataset from [here](https://nijianmo.github.io/amazon/index.html)
* Split the dataset into manageable chunks by running the <code>split_dataset.py</code> file as follows : 
  <code>sudo ${SPARK_HOME}/bin/spark-submit split_dataset.py Electronics.json output </code>
  
 * Upload the split files to S3

## Phase 2 - Training the model
* Setup an EMR cluster to run a pyspark script (refer [Assignment 5](https://coursys.sfu.ca/2022fa-cmpt-732-g1/pages/Assign5))
    ![EMR](/images/emr.jpg?raw=true  "EMR")
* Run the <code>model_creation.py</code> file (to create a model based on the split training data) on the cluster with a suitable configuration as follows:

	<code>spark-submit --deploy-mode cluster --conf spark.yarn.maxAppAttempts=1 s3://amazon-product-recommender/scripts/model_creation.py s3://amazon-product-recommender/ElectronicProductDataZIP/ s3://amazon-product-recommender/output</code>
	
* This will create a model that will be placed in S3

## Phase 3 - Scraping Amazon data

* ![Lambda](/images/lambda.jpg?raw=true  "Lambda")
* This script was run as an AWS lambda function. However, if required, it can be run locally as follows:
* Firstly, add your AWS_ACCESS_KEY, AWS_SECRET_KEY and AWS_SQS_QUEUE_NAME in <code>scraper.py</code> after creating a queue in SQS.
* Run the python scraper script with the below command :

  <code>python3 scraper.py</code>
  This script scrapes the required data from [Amazon.ca](https://www.amazon.ca/) and pushes it to the SQS queue.
* You should be able to see some messages in your SQS queue 
	![SQS Screenshot 1](/images/sqs_screenshot1.png?raw=true  "SQS Screenshot 1")

## Phase 4 - Ingesting SQS message queue into DB

* Initialize an EC2 instance and setup **MySQL Server**. Add the DB credentials to the <code>sentiment_analyzer.py</code> file

    ![EC2](/images/ec2.jpg?raw=true  "EC2")
* Add your AWS_ACCESS_KEY, AWS_SECRET_KEY and AWS_SQS_QUEUE_NAME to <code>sentiment_analyzer.py</code>
* Run the <code>sentiment_analyzer.py</code> using the following command :

	<code>python3 sentiment_analyzer.py  path_to_model</code>
* This file will ingest all messages from the SQS queue and run them through the model, then the predicted output labels are correspondingly modified in the MySQL DB.

## Phase 5 - Visualizing the data on Grafana

* On the same EC2 instance, setup a **Grafana** server.
* In the grafana server, initialize a data-source as your already existing MySQL server. 

* Import the <code>grafana_dashboard.json</code>, you should now be able to visualize the latest data from the database.
