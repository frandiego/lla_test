**Case definition**

You are required to implement a pipeline for creating an ABT (Analytics Base Table). You need to process a large data source that contains the customer location coordinates, comuna and event tag. 

The data source is uploaded daily to an S3 bucket in parquet format, the data is partitioned by day following the conventions of Hive metastore.

You need to calculate the distance (it can be Euclidean, Manhattan or any other method) for all customers in ***geo table*** to the customers with events in the ***labels table*** in 50 meters around, keep in mind that the number of combinations could increase rapidly, define a strategy to handle this number of combinations, the strategy should consider the right tool and the right coding strategy. 

Once the data is processed, it must be available to the data scientist who will process it through Jupyter notebooks, and it must be available to the data analyst who will process the data through SQL.

**Exercise**  

**Point 1:** (Architecture)

You must define the architecture for this pipeline, you can propose the architecture on AWS or GCP. Consider that this architecture should support the addition of similar data sources in later versions taking into account the following points:

- Storages tools and layers that are required (Raw, Staging, Analytics, Curated)
- Tools for transforming the data (Should be cost effective solution, EMR, Glue, Dataflow, DataProc)
- Define the orchestration tool (For instance: Airflow or Step Functions)
- Define the notification and alert tool (For instance: Airflow operator or SNS)
- Define the tool for Data Scientists (For instance: AI Platform, EC2, SageMaker)
- Define the tool for Business Analytics (For instance: BigQuery, Hive, Athena, RedShift)
- Any other required in your design

**Point 2:** (ETL Script)

Develop a PySpark, Spark or Scala script to process the sample (500K approx.) provided for this exercise, remember to create a script as efficiently as possible because this should work with full data (millions of records). 

The sample data is available in a shared Google Drive, in parquet files format in two different folders and structures; **geo** and **labels,** the following images show the table structures and a code example for importing the data into a Google Colab environment

![Text

Description automatically generated with low confidence](Aspose.Words.9a1f22da-203c-4c26-bb31-0ccf2321b65c.001.png)

![Graphical user interface, text, application

Description automatically generated](Aspose.Words.9a1f22da-203c-4c26-bb31-0ccf2321b65c.002.png)

Consider the following points to develop the script:

- The ETL process can be developed in the provided Colab notebook template, but a script is required as a delivery
- Join the datasets labels and geo through the ID Field
- Eliminate the duplicate *IDs*, and keep the first value for *comuna, latitude, longitude* and aggregate *event* field.
- Calculate the distance (it can be Euclidean, Manhattan or any other method) between all clients in 50 meters around. Keep in mind to define the efficient strategy at this point. Aggregate data by *comuna,* repartition tables and implement caching are some ideas for your strategy.
- Define the output table schema, this may be different for data scientist and data analysts depending on the architecture that you define in point 1.
- Save the output of the previous point thinking of having a weekly history in a file with the indicated format.

**Point 3:** (SQL Execution)

Connect the colab notebook to a SQLite database to perform data validation and make the data available for data scientists and bi analysts:

- You need to load the **label** table to do the exercise (the notebook contains an example of a loaded **geo** table).
- You must load the result table from point 1, and create basic queries to validate the joined tables (Data Quality).
- Create a query or a new table with the 20 communes with the highest number of type 2 events.
- Create a query or a new table with type 1 events by commune and calculate the avg, max and min of latitude and longitude by commune.





