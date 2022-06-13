---
marp: true
theme: default
_class: invert
paginate: true
---

# Predicting pedestrian count in Heidelberg's hauptstraße

###### A pipeline for consumption, processing and visualization of pedestrian and weather data

![h:350][data engineering]

<!-- footer: 'Data Engineering II • Prof. Frank Schultz • Team 1: Fatemeh Karampanah, Gonçalo Coutinho, Ömer Ates & Sasha Behrouzi'-->
---

<!-- _footer: ''-->

# Index

1. Introduction

2. Pipeline

3. Pedestrian data ingestion - Fatemeh

4. Weather data ingestion - Sasha

5. Data storage in BigQuery - Sasha

6. Weather forecasting - Ömer

7. Prediction Model - Sasha

8. Data visualization - Gonçalo

9. Q&A

---

# 1. Introduction

Data sources:
- hystreet.com
- openweathermap.org

Outcomes:
- Analise historical data
- Predict pedestrian count

---

# 2. Pipeline

<!-- _footer: ''-->

![h:600][pipeline]

---

# 3. Pedestrian data ingestion

- Writing the code to get data from API
- Produce messages using kafka producer and fetch in Kafka consumer

![][pedestrian]

---
*As the API key was not provided by the website we had to adapt and use csv format*
- Getting csv from the website and do the preparation process on the data 
- reading and passing the data row by row into Kafka producer
- subscribing the topic using Kafka consumer and converting the message to json
- dumping the data into mongodb

![h:300][pedestrian2]

---

# 4. Weather data ingestion

- Current weather data is retrieved from the API provided by openweathermap.org
- The response of the HTTP API call is a JSON document, then sent to the Kafka topic
- The consumer side is subscribed to the topic "weather" and after reading every message, stores it in the MondoDB collection "weather"
- The script is deployed in google cloud and run as a docker container. Then the container is scheduled to be run every 3 minutes with the cloud scheduler service.

![][weather_pipeline]

---

# 5. Data storage in BigQuery
###### 5.1 Data storage of weather collection via Google's Pub/Sub utilizing data flow

- Topic "weather" created in Pub/Sub
- Bucket created in data storage for temporary storage used by dataflow
- BigQuery table "openweathertable" created
- DataFlow job created based on Pub/Sub topic to BigQuery template. It will read a message from a weather topic and store the messages in the BigQuery table.

![][big_query]

---
<!-- _footer: ''-->
###### 5.2 Data storage of pedestrian collection directly to BigQuery table

- At this stage, data is read to a list of dictionaries from the MongoDB collection and appended to a list. 
- The list of records is used to insert rows of data into the pedestrian table later. In the python script pymongo and google. cloud. bigquery libraries are used.
- For granting access to the python script JSON key is created from the cloud service and passed as a credential to the python script.
- To automate the operation this application is triggered every 12 hours by a google schedule

![h:270][big_query2]

---

# 6. Prediction model
###### 6.1 Model selection

- Feature selection based on correlation table
- Created a random forest model based on: temperature, humidity, clouds, wind speed and rain
- Saved the model in a joblib format

---

![model]

---

# 6. Prediction model
###### 6.2 Model deployment

- Deployed the model in cloud run
- Python script in cloud run in reading data from BigQuery and re-writes the predictions in the same table

---

![bg h:600][model_pipeline]

---


# 7. Weather forecasting
- Architecture of DataFlow
- API Call in and Data ingestion
- Service Creation in CloudRun
- Job Creation and Deployment in CloudRun
- Table and View Creation of Master Table in BigQuery

![h:300][weather_forecast]


---

# 8. Data visualization

Steps:
- Create views in BigQuery
- Load data sources into Data Studio
- Create report

Report can be found [here][pedestrian report]

---

# 9. Q&A

# Thank you! Questions? <!--fit-->

Github can be found [here][github]
Final report can be found [here][final_report]


[//]: #

[github]: <https://github.com/Rrschch-6/Kafka-Project>
[pedestrian report]:  <https://datastudio.google.com/reporting/aee533d4-021b-4736-845d-422dd03448d1>
[data engineering]: <data_eng.png>
[data engineering 2]: <data_eng.jpg>
[pipeline]: <pipeline.png>
[weather_pipeline]: <weather_data_ingestion.png>
[big_query]: <bigquery_storage.png>
[big_query2]: <bigquery_storage2.png>
[model]: <model.png>
[model_pipeline]: <model_pipeline.png>
[weather_forecast]: <weather_forecasting.jfif>
[pedestrian]: <pedestrian_data.jfif>
[pedestrian2]: <pedestrian_data2.jfif>
[final_report]: <https://docs.google.com/document/d/1Rf42hOoHDahtLq7hzYmroTTFkqTLDOm9XoEFYdNv72U/edit?usp=sharing>
