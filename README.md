# multi-stage-sentiment-analysis-Mapreduce_hive
This is Multi-Stage Sentiment Analysis on Historical Literature application is developed with map reduce and hive
# Word-Based Sentiment Scoring and Trend Analysis on Historical Texts

## **Project Objective**
Through the processing of digitized texts from the 18th and 19th centuries, this project examines sentiment trends in historical literature. There are several books in the collection, and each one has metadata like the book ID, title, and publishing year. Hadoop MapReduce in Java and Hive for data processing, analysis, and visualization are used to create a multi-stage processing pipeline. The objectives include:

- **Data Extraction & Cleaning:** Extract book metadata and preprocess text.
- **Word Frequency Analysis:** Tokenize and lemmatize words.
- **Sentiment Analysis:** Assign sentiment scores using sentiment lexicons.
- **Trend Analysis:** Aggregate scores and word frequencies over time.
- **Bigram Analysis:** Implement a Hive UDF to extract and analyze bigrams.

## **Task 1: Preprocessing MapReduce Job**

### **Objective**
Clean and standardize the raw text data from multiple books while extracting essential metadata (book ID, title, year of publication).

### **Steps to Execute Task 1**

#### **1. Start Docker Containers**
```sh
docker compose up -d
```
This command initializes the Hadoop ecosystem using Docker.

#### **2. Compile Java Files and Create a JAR**
Copy the JAR file to your Hadoop environment:
```sh
mvn install
scp target/historical-text-sentiment-analysis-1.0.jar user@hadoop-master:/path/to/jars/
```

#### **3. Copy Input Data to HDFS**
```sh
hadoop fs -mkdir -p /input/dataset
hadoop fs -put .//input/my_file.csv /input/dataset/
```
This uploads the dataset and the JAR file into Hadoop's distributed file system.

#### **4. Run the MapReduce Job**
```sh
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/DataCleaningMapReduce-1.0-SNAPSHOT.jar com.project.driver.BookPreprocessingDriver \
  /input/dataset/my_file.csv /output
```
This processes the input text and generates the cleaned dataset.

#### **5. Verify Output in HDFS**
```sh
hdfs dfs -ls /user/root/output/
```
Ensure that output files (`part-r-00000` and `_SUCCESS`) are created.

#### **6. View Results**
```sh
hdfs dfs -cat /user/root/output/part-r-00000
```
This command displays the processed output.

#### **7. Copy Output to Local System**
```sh
hadoop fs -cat /output/*
hdfs dfs -get /output /opt/hadoop-3.2.1/share/hadoop/mapreduce/

```
This retrieves the output from HDFS to the local filesystem for further analysis.

## **Next Steps**
The output from Task 1 will serve as input for subsequent tasks.

---
## **Task 2: Word Frequency Analysis with Lemmatization**

### **Objective**
Computing word frequencies by splitting each sentence into words and applying lemmatization to reduce words to their base forms

### **Steps to Execute Task 1**

#### **1. Start Docker Containers**
```sh
docker compose up -d
```
This command initializes the Hadoop ecosystem using Docker.

---

#### **2. Building the Java Code with Maven**
Before running the MapReduce job, compile the Java source files and package them into a JAR file by using maven :
```sh
mvn clean install
```
This command will generate a JAR file in the target/ directory, which contains your MapReduce code.

---

#### **3. Preparing Input Data Files **

The input that is output from Task1 is located in the output/ folder of the repository.

---

#### **4. Moving the Jar and Input Files to the Docker Container**
#### **4.1 Moving the JAR File to the Container**

Copy the built JAR file to the ResourceManager container. 

```bash
docker cp target/word-frequency-lemmatization-1.0-SNAPSHOT-jar-with-dependencies.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```


#### **4.2 Moving the Input File to the Container**

Next, copy the Word frequency lemmatization dataset to the ResourceManager container:

```bash
docker cp output/part-r-00000 resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```
---

### 5. **Connect to the ResourceManager Container**

To run the Hadoop commands, we are connected to the ResourceManager container:

```bash
docker exec -it resourcemanager /bin/bash
```

Once inside the container, navigated to the Hadoop directory where our files were copied:

```bash
cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

---

### 6. **Set Up HDFS for Input File**

To run the MapReduce job, the input file needs to be stored in Hadoop’s distributed file system (HDFS).

#### **6.1 Creating Directories in HDFS**

Created a directory in HDFS for the input file by running the following command:

```bash
hadoop fs -mkdir -p /input/dataset2
```

#### **6.2 Uploading the Input File to HDFS**

Uploaded the output of task1 file to HDFS:

```bash
hadoop fs -put part-r-00000 /input/dataset2/
```

---

### 7. **Executing the Word Frequency Lemmatization MapReduce Jobs**

Run the job using the following command:

```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/word-frequency-lemmatization-1.0-SNAPSHOT-jar-with-dependencies.jar Task2.WordFreqLemmatizationDriver /input/dataset2/part-r-00000 /output2
```

This command will execute the MapReduce job with the Cleaned data as the input and store the results in the `/output2` directory in HDFS.

---

### 8. **View the Output of the MapReduce Job**

The output will be stored in multiple directories (one for each task). We can view the output files using the following commands:

```bash
hadoop fs -ls /output
```

```bash
hadoop fs -cat /output/task2/part-r-00000
```
---

### 9. **Copy Output from HDFS to Local OS**

Once you have verified the results, copying the output from HDFS to your local file system.

Use the following command to copy the output from HDFS to the Hadoop directory:

```bash
hdfs dfs -get /output2 /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```
Now, exit the ResourceManager container:

```bash
exit
```
This retrieves the output from HDFS to the local filesystem for further analysis

---
## **Next Steps**
The output from Task 2 will serve as input for subsequent tasks.

## Task 3: Sentiment Scoring

### **Objective:
Task 3 aims to Assign sentiment scores to texts by mapping words (or their lemmatized forms) to sentiment values using a sentiment lexicon, ensuring that scores are traceable to individual books.
Given Instructions are :
Mapper:
Input: The output from Task 1 or Task 2.
Lexicon Matching: For each token (or lemma), check its presence in the chosen sentiment lexicon (e.g., AFINN or SentiWordNet) and retrieve the corresponding sentiment score.
Output: Emit key-value pairs where the key is a composite of (bookID, year) and the value is the sentiment score for that token.
Reducer:
Aggregation: Sum the sentiment scores for each key to obtain an overall sentiment score for each book and year.
Output: Produce a dataset mapping each book (and its year) to a cumulative sentiment score.


#### **1. Start Docker Containers**
```sh
docker compose up -d
```
This command initializes the Hadoop ecosystem using Docker.

---

#### **2. Building the Java Code with Maven**
Before running the MapReduce job, compile the Java source files and package them into a JAR file by using maven :
```sh
mvn clean install
```
This command will generate a JAR file in the target/ directory, which contains your MapReduce code.

---

#### **3. Preparing Input Data Files **

The input that is output from Task2 is located in the output/ folder of the repository.

---

#### **4. Moving the Jar and Input Files to the Docker Container**
#### **4.1 Moving the JAR File to the Container**

Copy the built JAR file to the ResourceManager container. 

```bash
docker cp target/word-sentiment-score-1.0-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```


#### **4.2 Moving the Input File to the Container**

Next, copy the Word frequency  dataset to the ResourceManager container:

```bash
docker cp output/task2/part-r-00000 resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```
---

### 5. **Connect to the ResourceManager Container**

To run the Hadoop commands, we are connected to the ResourceManager container:

```bash
docker exec -it resourcemanager /bin/bash
```

Once inside the container, navigated to the Hadoop directory where our files were copied:

```bash

```
cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/

---

### 6. **Set Up HDFS for Input File**

To run the MapReduce job, the input file needs to be stored in Hadoop’s distributed file system (HDFS).

#### **6.1 Creating Directories in HDFS**

Created a directory in HDFS for the input file by running the following command:

```bash
hadoop fs -mkdir -p /input/dataset3
```

#### **6.2 Uploading the Input File to HDFS**

Uploaded the output of task1 file to HDFS:

```bash
hadoop fs -put part-r-00000 /input/dataset3
```

---

### 7. **Executing the Word Frequency Lemmatization MapReduce Jobs**

Run the job using the following command:

```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/word-sentiment-score-1.0-SNAPSHOT.jar Task3.WordSentimentScoreDriver /input/dataset3/part-r-00000 /output3
```

This command will execute the MapReduce job with the Cleaned data as the input and store the results in the `/output3` directory in HDFS.

---

### 8. **View the Output of the MapReduce Job**

The output will be stored in multiple directories (one for each task). We can view the output files using the following commands:

```bash
hadoop fs -ls /output
```

```bash
hadoop fs -cat /output/task2/part-r-00000
```
---

### 9. **Copy Output from HDFS to Local OS**

Once you have verified the results, copying the output from HDFS to your local file system.

Use the following command to copy the output from HDFS to the Hadoop directory:

```bash
hdfs dfs -get /output3 /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```
Now, exit the ResourceManager container:

```bash
exit
```
This retrieves the output from HDFS to the local filesystem for further analysis



---


## Task 4: Trend Analysis & Aggregation

### **Objective**
Task 4 aims to find long-term trends and possible connections with past events by combining sentiment scores and word frequencies over larger time periods (e.g., by decade). The data will be processed and aggregated using a Hadoop MapReduce operation, which will provide sentiment trends over time.

#### **Steps to Execute Task 4**

#### **1. Start Docker Containers**
```sh
docker compose up -d
```

#### **2. Build the Project with Maven**
```sh
mvn install
```

#### **3. Access the Resource Manager**
```sh
docker exec -it resourcemanager bash
```

#### **4. Create Directory for MapReduce Files**

```sh
mkdir -p /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```


#### **5. Exit Resource Manager Container**

```sh
exit
```


#### **6. Copy JAR File to Resource Manager**

```sh
docker cp target/word-Trend-Analysis-1.0-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

#### **7. Copy Task 3 Output to Resource Manager**

```sh
docker cp output/task3/part-r-00000 resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```


#### **8. Reconnect to Resource Manager**

```sh
docker exec -it resourcemanager bash
```


#### **9. Create Input Directory in HDFS**

```sh
cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/
hadoop fs -mkdir -p /input/dataset4
```


#### **10. Upload Task 3 Output to HDFS**

```sh
hadoop fs -put part-r-00000 /input/dataset4
```


#### **11. Run the MapReduce Job for Task 4**

```sh
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/word-Trend-Analysis-1.0-SNAPSHOT.jar Task4.WordTrendAnalysisDriver /input/dataset4/part-r-00000 /output4
```


#### **12. Retrieve Output from HDFS**

```sh
hdfs dfs -get /output4 /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```


#### **13. Exit the Resource Manager Container**

```sh
exit
```


#### **14. Copy the Output to Local System**

```sh
docker cp resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/output4/ task4/
```


### Note:
We've used the same XML file for Task 2, Task 3, and Task 4, specifying the appropriate class name for each task within the file.
### TASK 5
Step 1: Copy Files into Hive Server

Certainly! Here's the updated README with the additional code you requested:

---

# Task 5: Bigram Analysis Using Hive UDF

## Objective

The goal of this task is to extract and analyze bigrams (two-word combinations) from lemmatized text data using a custom Hive UDF (User-Defined Function). This process involves the following steps:
1. **Uploading the UDF (BigramUDF.java) into the Hive server.**
2. **Compiling the UDF and packaging it into a JAR file.**
3. **Registering the UDF in Hive.**
4. **Creating tables to store and process the data.**
5. **Running the bigram analysis and storing the results in HDFS.**

---

## Prerequisites

Before running the following steps, ensure that:
- **Hive** is installed and configured properly on your system or cluster.
- **Hadoop** is correctly set up for HDFS operations.
- The **BigramUDF.java** file is available on your local system.

---

## Steps for Task 5

### **Step 1: Copy Files into Hive Server**

Start your Hive server using Docker and copy the necessary files (in this case, `BigramUDF.java`) into the Hive container.

```sh
docker compose up -d
docker cp BigramUDF.java hive-server:/opt/hive
```

This command starts the Hive server in detached mode and copies the `BigramUDF.java` file into the `/opt/hive` directory inside the container.

---

### **Step 2: Access the Hive Server**

Log into the Hive container to access the Hive environment.

```sh
docker exec -it hive-server /bin/bash
```

This command will open a terminal session inside the Hive server container.

---

### **Step 3: Upload Files to HDFS**

Navigate to the `/opt/hive` directory inside the container and upload the `BigramUDF.java` file to HDFS.

```sh
cd /opt/hive
hdfs dfs -put BigramUDF.java /user/hive/warehouse
```

This command uploads the `BigramUDF.java` file to HDFS, making it available for compilation and usage.

---

### **Step 4: Compile Java UDF**

Next, compile the `BigramUDF.java` file using the `javac` compiler. Ensure the classpath is set correctly for your system’s Hadoop and Hive paths.

```sh
javac -cp "/etc/hadoop:/opt/hadoop-2.7.4/share/hadoop/common/lib/:/opt/hadoop-2.7.4/share/hadoop/common/:/opt/hadoop-2.7.4/hadoop/hdfs:/opt/hadoop-2.7.4/share/hadoop/hdfs/lib/:/opt/hadoop-2.7.4/share/hadoop/hdfs/:/opt/hadoop-2.7.4/share/hadoop/yarn/lib/:/opt/hadoop-2.7.4/share/hadoop/yarn/:/opt/hadoop-2.7.4/share/hadoop/mapreduce/lib/:/opt/hadoop-2.7.4/share/hadoop/mapreduce/:/opt/hive/lib/*" -d . BigramUDF.java
```

This will compile the `BigramUDF.java` file and generate the corresponding `.class` files.

---

### **Step 5: Package into a JAR**

After compiling the Java file, create a JAR file from the compiled `.class` file for the UDF.

```sh
jar -cvf bigram_udf.jar BigramUDF.class
```

This step packages the compiled class into a JAR file named `bigram_udf.jar`.

---

### **Step 6: Start Hive CLI**

Launch the Hive command-line interface (CLI) to interact with the Hive environment.

```sh
hive
```

This will open the Hive CLI, where you can run Hive commands and queries.

---

### **Step 7: Register UDF in Hive**

Once you're inside the Hive CLI, register the UDF by adding the JAR file and creating a temporary function for bigram counting.

```sql
-- Add the UDF JAR file to Hive
ADD JAR /opt/hive/bigram_udf.jar;

-- Register the custom Bigram UDF
CREATE TEMPORARY FUNCTION bigram_count AS 'BigramUDF';
```

This step ensures that the `BigramUDF` is available to Hive for use in queries.

---

### **Step 8: Create and Load Data into Hive Tables**

Create a table to store the lemmatized data. This table will hold information about the lemmatized words, their frequencies, and associated metadata (like book name and publication year).

```sql
-- Create the table to store lemmatized data
CREATE TABLE lemma_data (
  book_name STRING,
  lemma_word STRING,
  year INT,
  count INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',';
```

Then, load your data into the table from the specified HDFS path where the lemmatized files are stored.

```sql
-- Load data into the table from HDFS
LOAD DATA INPATH '/user/hive/inputs/*' OVERWRITE INTO TABLE lemma_data;
```

This will load the lemmatized data from the HDFS path (`/user/hive/inputs/*`) into the `lemma_data` table.

---

### **Step 9: Perform Bigram Analysis**

Now, you can use the `bigram_count` function to analyze the bigrams from the `lemma_data` table. You can follow the example below to extract and analyze bigrams:

```sql
-- Drop the existing bigram output table if it exists
DROP TABLE IF EXISTS bigram_output;

-- Create the bigram output table
CREATE TABLE bigram_output AS
SELECT
  book_name,
  year,
  bigram_count(lemma_word) AS bigram,  -- Use the UDF to extract bigrams
  count
FROM lemma_data
GROUP BY book_name, year, lemma_word;
```

This will create a table called `bigram_output` containing the `book_name`, `year`, `bigram`, and their associated `count` values.

---
. Most Frequent Bigrams Across All Books
sql
Copy
Edit
SELECT bigram, COUNT(*) AS total_frequency
FROM lemmatized_books
LATERAL VIEW explode(extract_bigrams(text)) exploded_table AS bigram
WHERE
  split(bigram, ' ')[0] NOT IN (
    'the','and','a','of','in','is','to','that','with','as','on','for','by','at',
    'from','was','are','this','but','his','her','who','which','what','all','no',
    'he','she','it','you','i','we','said','do','does','did','be','been','being',
    'have','has','had','s'
  )
  AND
  split(bigram, ' ')[1] NOT IN (
    'the','and','a','of','in','is','to','that','with','as','on','for','by','at',
    'from','was','are','this','but','his','her','who','which','what','all','no',
    'he','she','it','you','i','we','said','do','does','did','be','been','being',
    'have','has','had','s'
  )
GROUP BY bigram
ORDER BY total_frequency DESC
LIMIT 50;
In this query, we:

Extracted bigrams using the LATERAL VIEW and the explode() function.

Removed common stopwords like "the", "and", "is" from both the first and second words of the bigram to focus on meaningful combinations.

Counted the occurrences of each bigram and ranked them by frequency, ensuring that we retrieve only the 50 most frequent bigrams across the entire dataset.

2. Top 5 Bigrams per Book (Grouped by Book)
sql
Copy
Edit
INSERT OVERWRITE DIRECTORY '/user/hive/output/top_bigrams_per_book'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT book_id, bigram, frequency
FROM (
  SELECT book_id, bigram, frequency, ROW_NUMBER() OVER (PARTITION BY book_id ORDER BY frequency DESC) AS rank
  FROM (
    SELECT book_id, bigram, COUNT(*) AS frequency
    FROM (
      SELECT book_id, explode(extract_bigrams(text)) AS bigram
      FROM lemmatized_books
    ) raw
    WHERE
      split(bigram, ' ')[0] NOT IN (
        'the','and','a','of','in','is','to','that','with','as','on','for','by','at',
        'from','was','are','this','but','his','her','who','which','what','all','no',
        'he','she','it','you','i','we','said','do','does','did','be','been','being',
        'have','has','had','s'
      )
      AND
      split(bigram, ' ')[1] NOT IN (
        'the','and','a','of','in','is','to','that','with','as','on','for','by','at',
        'from','was','are','this','but','his','her','who','which','what','all','no',
        'he','she','it','you','i','we','said','do','does','did','be','been','being',
        'have','has','had','s'
      )
    GROUP BY book_id, bigram
  ) grouped
) ranked
WHERE rank <= 5;
Here, we:

Used the ROW_NUMBER() window function to rank bigrams within each book based on frequency.

Filtered to return only the top 5 bigrams for each book, ensuring that no single book disproportionately affects the overall ranking.

Stored the results in an output directory for further analysis.

3. Top 5 Bigrams per Decade (Grouped by Decade)
sql
Copy
Edit
INSERT OVERWRITE DIRECTORY '/user/hive/output/top_bigrams_per_decade'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT decade, bigram, frequency
FROM (
  SELECT
    decade,
    bigram,
    frequency,
    ROW_NUMBER() OVER (PARTITION BY decade ORDER BY frequency DESC) AS rank
  FROM (
    SELECT
      decade,
      bigram,
      COUNT(*) AS frequency
    FROM (
      SELECT
        CONCAT(FLOOR(year / 10) * 10, 's') AS decade,
        exploded_table.bigram AS bigram
      FROM lemmatized_books
      LATERAL VIEW explode(extract_bigrams(text)) exploded_table AS bigram
    ) raw_bigrams
    WHERE
      split(bigram, ' ')[0] NOT IN (
        'the','and','a','of','in','is','to','that','with','as','on','for','by','at',
        'from','was','are','this','but','his','her','who','which','what','all','no',
        'he','she','it','you','i','we','said','do','does','did','be','been','being',
        'have','has','had','s'
      )
      AND
      split(bigram, ' ')[1] NOT IN (
        'the','and','a','of','in','is','to','that','with','as','on','for','by','at',
        'from','was','are','this','but','his','her','who','which','what','all','no',
        'he','she','it','you','i','we','said','do','does','did','be','been','being',
        'have','has','had','s'
      )
    GROUP BY decade, bigram
  ) ranked
) final
WHERE rank <= 5;
For the decade-based analysis, we:

Grouped the bigrams by decade by converting the year column to a decade format (e.g., 1800 → 1800s).

Applied the same stopword filtering, bigram counting, and ranking methodology used in previous queries.

Extracted the top 5 most frequent bigrams per decade to show trends over time.
### **Step 10: Export Results to HDFS**

Finally, export the bigram analysis results to HDFS for further processing or downstream analysis.

```sql
-- Export the bigram output to HDFS
INSERT OVERWRITE DIRECTORY 'hdfs://namenode:8020/output/part-r-00000'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT book_name, year, bigram, count
FROM bigram_output;
```


