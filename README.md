# IDSTune: A Multi-Agent Collaborative Framework for Integrated Database System Tuning

This is the source code to the paper **"IDSTune: A Multi-Agent Collaborative Framework for Integrated Database System Tuning"**. Please refer to the paper for the experimental details.

## Table of Content
- [IDSTune: A Multi-Agent Collaborative Framework for Integrated Database System Tuning](#idstune-a-multi-agent-collaborative-framework-for-integrated-database-system-tuning)
  - [Table of Content](#table-of-content)
  - [Environment Installation](#environment-installation)
  - [Workload Preparation](#workload-preparation)
    - [SYSBENCH](#sysbench)
    - [Join-Order-Benchmark (JOB)](#join-order-benchmark-job)
    - [TPCC and TPC-DS](#tpcc-and-tpc-ds)
  - [Quick Start](#quick-start)

## Environment Installation

In our experiments,  We conduct experiments on PostgreSQL 15.1.

1. Preparations: Python == 3.10

2. Install packages

   ```shell
   pip install -r requirements.txt
   pip install .
   ```

3. Download and install PostgreSQL 15.1 and boost
4. 
   ```shell
   sudo apt update
   sudo apt install postgresql-client-15 postgresql-15
   systemctl status postgresql
   ```

## Workload Preparation 

### SYSBENCH

Download and install

   ```shell
   git clone https://github.com/akopytov/sysbench.git
   ./autogen.sh
   ./configure
   make && make install
   ```

Load data

   ```shell
    sysbench --db-driver=pgsql \
    --pgsql-host=$HOST \
    --pgsql-port=$PG_PORT \
    --pgsql-user=postgres \
    --pgsql-password=$PASSWD \
    --pgsql-db=sbtest \
    --table-size=800000 \
    --tables=150 \
    --events=0 \
    --threads=32 \
    oltp_read_write prepare > sysbench_prepare.out
   ```

### Join-Order-Benchmark (JOB)

Download IMDB Data Set from http://homepages.cwi.nl/~boncz/job/imdb.tgz.

Follow the instructions of https://github.com/gregrahn/join-order-benchmark to load data into PostgreSQL.

### TPCC and TPC-DS
Follow the instructions of https://www.tpc.org/default5.asp to prepare TPC benchmarks.

## Quick Start
1. Modify related settings (e.g., API key, benchmark, DB login info...) in `config.ini`
2. Execute Phase I: workload compression to obtain selected features.
    ```shell
    cd ./workload_compression
    
    #Parse the workload to obtain statistical information
    python WorkloadParser.py

    #Select the most relevant features
    python get_feature_list.py

    #Extract detailed feature values
    python get_features.py
   ``` 
3. Execute Phase II: configuration recommendation to obtain optimal configurations.
    ```shell
    cd ../configuration_recommendation
    ./configuration_recommendation/optimize.py
   ``` 