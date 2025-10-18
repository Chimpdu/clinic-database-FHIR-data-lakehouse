# Clinic database app and clinical data lakehouse



## Introduction

This repository consists of two parts: 
* A clinic database app allowing CRUD operations on FHIR data from the KTH Hapi FHIR server (https://hapi-fhir.app.cloud.cbh.kth.se/fhir/swagger-ui/) and messaging between patients and their practitioners.  **(./clinic_application)**
* A data lakehouse keeping replicated data from the KTH Hapi FHIR server and using tools including Apache Hudi and Spark to support querying. **(./python-lakehouse)**

## Clinic database app
### How it works:
The application fetches FHIR data from the KTH Hapi FHIR server and parse the data to display for admin and normal users; admin users have the priviledge to add, delete and update data. 

The application has 6 pages: **Patient (Patient resource type); Practitioner (Practitioner resource type); Appointment (Encounter resource type); Observation (Observation resource type); Diagnosis (Condition resource type); Clinic (Organization resource type)**. We support submitting files in Observation and Diagnosis pages and files are stored as (Media resource type)



A **default admin user** account is :
* Username: admin
* Password: admin

For each patient and practitioner registered in the server, an user account is given. Practitioners have the privilege to view the data, message the patients who they have appointment with, and manage their accounts. Patients can message the practitioners who they have appointment with and manage their accounts.

A **default practitioner** account is :
* Username: "d" + practitioner ID (For instance, d9003)
* Password: "d" + practitioner ID (For instance, d9003)

A **default patient** account is :
* Username: "p" + patient ID (For instance, p9003)
* Password: "p" + patient ID (For instance, p9003)

### How to run:
#### Windows:

 Go to **clinic_application** folder
 ``` bash
 pip install -r requirements.txt
 python login.py
```
## Clinical data lakehouse

### Compose Files

This repository includes five[^*] different Docker Compose configurations that define various parts of the data-lake setup.

* [`base.compose.yaml`](./base.compose.yaml):
  Defines the **core data lake components**, such as the [`namenode` and `datanode`](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html#NameNode+and+DataNodes). It also includes services required for **Hive** and **Spark**, like [`sparkmaster`]() and [`spark-worker-1`]().

* [`hive.compose.yaml`](./hive.compose.yaml):
  Includes services required for **Hive**.

* [`spark.compose.yaml`](./spark.compose.yaml):
  Adds **Spark**, [`sparkmaster`]() and [`spark-worker-1`]().

* [`trino.compose.yaml`](./trino.compose.yaml):
  Adds optional services needed to enable **[Trino](https://trino.io/)** as a high-performance SQL query engine for interactive reads on the data lake.

* [`local.compose.yaml`](./local.compose.yaml):
  Wraps both the other compose files + adds a `traefik` reverse proxy.

---

[^*]: There are actually six different docker compose configurations, but the last one is only used for deploying to [the cloud](https://cloud.cbh.kth.se/) with the [`kthcloudw`](./kthcloudw) wrapper script.

### Code

The code is in **python-lakehouse folder** and we perform queries on the data lake using **Apache Spark** and **Apache Hudi**.

### How to run
#### Prerequisites

- docker
- vs-code
- [vs-code remote development extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.vscode-remote-extensionpack)
### 0. Make docker running and open this repository in the VS code



* In **VS Code**, and press (`ctrl` + `shift` + `p`) or (`cmd`+ `shift`+ `p` on mac) and select `Dev Containers: Rebuild and Reopen in Container`.

### 1. Start the Data Lake Environment

```bash
make clean up logs
# clean and logs are optional
# logs will run "docker compose -f local.compose.yaml -f" to show all logs from the datalake in your c
```

### 2. Replicate data from the KTH Hapi FHIR server to our local data lakehouse



```bash
pip install requests
make python/create 
```
This will load all non-empty resource types to our data lakehouse

### 3. Query records from our local data lakehouse


```bash
make python/read
```
Using the command line tool, we can specify to query which resource type and how many records we want. The results will be saved into **outputs folder** in csv, jsonl, and parquet formats

### 4. Tear Down

When you’re done:

```bash
make down # optionally add "clean" to remove all docker volumes
```



### Credits
Portions of the data-lake configuration and examples are adapted from
cm2027, *lab3-datalake*, GitHub repository.
Source: https://github.com/cm2027/lab3-datalake (accessed Oct 18, 2025).
