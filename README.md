# stream-DaQ-experiments
Experimental Analyses for the Stream DaQ framework. 

## TL;DR
To reproduce the experimental analysis simply run the following command on a terminal (Linux):
```bash
bash run-experiments.sh
```

# Prerequisites
To comprehensively reproduce the experimental analyses, docker installation is the only software requirement. In case you do not have Docker installed on your machine, please refer to the [official Docker documentation page](https://docs.docker.com/engine/). 

Also, to unlock the total capabilities of the open source [Pathway engine](https://github.com/pathwaycom/pathway) running at the heart of Stream DaQ, it is highly recommended acquiring a free Pathway API key from the [official website](https://pathway.com/get-license/) (free account required). The experimental analyses in this repository can be reproduced without having this API, but the Pathway's (therefore Stream DaQ's) capabilities are not unlocked to the full, leading to potential performance degradation. In our experimental analysis we ensure fair comparisons by using such an API key for the Stream DaQ's experiments.

# Preparatory Steps

## 1. Pathway API License Key

If you have a Pathway API key, please edit the last line of the `.env` file by replacing the current placeholder value with your acquired API key. For example, for an API key in the form of `ABCD0E-F1GH2I`, the `.env` file should look like the following:
```dotenv
# comment: head toward the very last line of the file
PATHWAY_LICENSE_KEY='ABCD0E-F1GH2I'
```

## 2. Reddit Comments May 2015 Dataset

To fully reproduce the experimental analyses, you can download the [Reddit Comments May 2015 Dataset](https://www.kaggle.com/code/kerneler/starter-may-2015-reddit-comments-0f049f5a-b/data) as a zipped `.sqlite` file. The `.sqlite` file can be exported to `csv` format following [these steps](https://deeplearning.lipingyang.org/export-sqlite-database-to-a-csv-file-using-sqlite3-command-line-tool-ubuntu-16-04/). Note that the name of the table contained in the `.sqlite` file is `May2015`. Once downloaded, unzipped and exported to `csv` format, move the dataset inside the `stream/datasets/` directory and change the `.env` file accordingly:
```dotenv
REDDIT_CSV_FILE="datasets/<the_name_of_your_exported_csv_dataset>.csv"
```

To accommodate users reproduce our experimental analyses without the need of downloading the whole (large) dataset file, we are providing a sample that contains 10K records as part of the existing source code in this repository. To reproduce the experimental analyses on this 10 subset, you need to do nothing more than executing the following command at the root directory of the repository:
```bash
bash run-experiments.sh
```

# Reproducing the experimental analyses

To reproduce our experimental analyses, please do the following steps:
1. (Optionally) change window configurations from the `.env` file;
2. (Optionally) change sub-stream lengths fom the `.env` file;
3. From the root directory of the project, run `bash run-experiments.sh`;
4. See the live results in the constantly updated `execution_results/stream-daq/final_performance.csv` file.

# Stop running experiments

To stop all running experiments, execute the following command at the root directory of the project:
```bash
docker compose down
```

Note that the experimental results in the `execution_results/stream-daq/final_performance.csv` file are persisted in the file system of your host machine (and not the Docker containers running the experiments). Running new experiments will simply append more lines at the end of the existing ones. The experimental setting configurations are logged in the csv folder for easy identification of experiments.

# Contact

For any issues reproducing the experimental analyses feel free to contact us at {my_GitHub_username_without_bil} [at] csd.auth.gr.
