#end-to-end-elt-pipline-


## ✅ **ELT Data Pipeline Description**

### 🔁 Overview:

This pipeline is designed to extract product data from Amazon using **Scrapy**, store raw data in **Amazon S3**, transform it using **AWS Lambda**, and then re-store the cleaned data into a **partitioned S3 bucket** for analytics using **AWS Glue and Athena**.

---

### 📥 1. **Extraction (E)** – *Scrapy Spider*

* The pipeline begins with a **Scrapy-based crawler** that scrapes product details (name, price, rating, etc.) from Amazon.
* The spider runs daily or on-demand and outputs data in **JSON** format.
* Raw data is pushed directly to a raw S3 bucket:

  ```
  s3://amazon-data-lake/raw/
  ```

---

### 🔧 2. **Loading Raw Data (L)** – *S3 Storage*
![s3](https://github.com/user-attachments/assets/bd909a9f-ae39-4e77-b353-49162d4e0358)

https://github.com/user-attachments/assets/38578e51-5d77-41e1-b8e9-ff2ec61c80b1

* Scraped files are organized and uploaded to:



  ```
  s3://amazon-data-lake/raw/YYYY-MM-DD/amazon_raw.json
  ```
* Each day’s crawl is stored in its own dated folder for traceability.

---

### 🔄 3. **Transformation (T)** – *AWS Lambda Function*


https://github.com/user-attachments/assets/b1df6ad0-4d0b-4549-9724-3569832e3e2c


* An **AWS Lambda** function is triggered upon object creation in the raw bucket.
* It performs cleaning and normalization tasks such as:

  * Ensuring data types (e.g., converting `rating` to `float`)
  * Removing duplicates or empty fields
  * Standardizing column names
* The transformed data is saved in **cleaned, partitioned folders** in another S3 bucket:

  ```
  s3://amazon-data-lake/clean/year=2025/month=07/day=05/cleaned_data.json
  ```


---

### 🧠 4. **Cataloging** – *AWS Glue Crawler*
![glue crawler](https://github.com/user-attachments/assets/9728a26c-2086-4a26-99a2-2919debec4c9)

* A **Glue Crawler** scans the cleaned S3 bucket.
* It infers the schema from the partitioned structure and updates the **AWS Glue Data Catalog**.
* This makes the data queryable in **Amazon Athena**.

---

### 🔍 5. **Querying** – *Amazon Athena*


https://github.com/user-attachments/assets/7a3041fa-ec6c-49ea-b0fd-138ee5a35f66


* Example query:

  ```sql
  SELECT name, current_price, rating
  FROM amazon_cleaned_data
  WHERE year = '2025' AND month = '07' AND day = '05'
    AND brand = 'Xiaomi';
  ```
* The use of partition filters ensures **optimized query performance and lower cost**.

---

## ✅ Technologies Used:

| Stage           | Tool/Service                         |
| --------------- | ------------------------------------ |
| Extract         | Scrapy (Python)                      |
| Raw Storage     | Amazon S3                            |
| Transform       | AWS Lambda                           |
| Cleaned Storage | S3 (partitioned by `year/month/day`) |
| Catalog         | AWS Glue Crawler                     |
| Query           | Amazon Athena                        |



