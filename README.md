# Development-of-a-Real-Time-Data-Pipeline-for-User-Profile-Analysis

# Kafka User Data Producer(producer.py)

This Python script is designed to produce user data to a Kafka topic using the Confluent Kafka library. It continuously fetches user data from the Random User Generator API and sends it to the specified Kafka topic. If there are any errors during the process, it will retry and log the error message.
## Usage
The script will continuously fetch user data from the Random User Generator API, serialize it as JSON, and produce it to the specified Kafka topic. It will also log the delivery status for each message.
If any request exceptions occur during data fetching, the script will retry after a brief delay.

## Code Explanation
The script imports necessary libraries: confluent_kafka.Producer, requests, json, and time.

It sets up Kafka producer configuration in the producer_config dictionary.

A Kafka producer instance is created using the configuration.

The get_user_data function fetches user data from the Random User Generator API. If there's a request exception, it retries after a 5-second delay.

The delivery_report function handles message delivery reports and logs any errors.

The main loop continuously fetches user data, serializes it as JSON, and produces it to the Kafka topic. It also triggers message delivery reports and flushes the producer. Messages are produced every 5 seconds.

In case of an error during message delivery, the script logs the error message and continues producing data.

# Kafka, Cassandra, and MongoDB Data Integration with PySpark

This PySpark script is designed for integrating data from Kafka, processing it, and storing it in both Cassandra and MongoDB databases. It performs various data transformations and filtering operations along the way.

## Usage 
The script reads data from a Kafka topic, processes it, and performs data transformations. It filters data based on age, and then it writes the transformed data to both Cassandra and MongoDB databases. The script provides streaming capabilities to continuously process incoming data.

## Code Explanation
Kafka Data Ingestion
The script initializes a Spark session and sets up the Kafka source. It reads data from the "user_data_topic" on the specified Kafka broker(s).

Data Transformation and Filtering
The incoming data is deserialized as JSON, and relevant fields are selected. The script calculates the age of users and applies various transformations to the data, such as creating full names and addresses. Users below 18 years of age are filtered out (RGPD).

Cassandra Integration
The script connects to a Cassandra database, creates a keyspace if it doesn't exist, and defines a table for storing user data. It then writes the filtered data to Cassandra in an append mode.

MongoDB Integration
The script integrates with a MongoDB database. It uses a custom "foreachBatch" function to write data to the specified MongoDB collection.

# User Insights Dashboard with Dash and MongoDB

This Python script creates a real-time dashboard using Dash, which visualizes user insights fetched from a MongoDB database. The dashboard displays age distribution, gender distribution, and the most common email domains of users. It updates the data every 10 seconds.

## Code Explanation
MongoDB Data Retrieval
The script connects to a MongoDB database and fetches user data from the "users" collection. It calculates various statistics for the dashboard, including the age distribution, gender distribution, and most common email domains.

Dash App Initialization
A Dash app is created with a Bootstrap-based layout. The layout includes cards displaying the total number of users and a brief description of the app. It also features three visualizations: age distribution, gender distribution, and most common email domains.

Dashboard Callback
The app includes a callback function that updates the visualizations and the total user count every 10 seconds (specified in milliseconds). It fetches updated data from MongoDB and recalculates the statistics for the charts. The figures are then updated, and the user count is displayed.

Running the Dashboard
The if __name__ == '__main__': block ensures that the app is only executed when the script is run directly. The app.run_server(debug=True) line starts the dashboard, and it can be accessed in a web browser.

# Documentation RGPD

In this project, we place a strong emphasis on respecting the General Data Protection Regulation (GDPR) by implementing stringent data privacy measures. It's important to note that we have not utilized any sensitive or personally identifiable information (PII) such as passwords, actual pictures, or phone numbers. Furthermore, to ensure GDPR compliance, we have taken the precaution of using data exclusively from users who are over the age of 18. This conscientious approach guarantees that the data used in our project aligns with GDPR guidelines, safeguarding user privacy and data protection throughout the entire project



