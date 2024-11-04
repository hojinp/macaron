# Reducing cross-cloud/region costs with the auto-configuring MACARON cache

This repository contains the prototype code for Macaron, as presented in our SOSP ’24 paper titled “Reducing Cross-Cloud/Region Costs with the Auto-Configuring MACARON Cache.” The main evaluation results discussed in the paper can be reproduced using our [simulator](https://github.com/hojinp/macaron_simulator) repository. Please note that this code has not undergone review as part of the Artifact Evaluation.


## Table of Contents
- [Reducing cross-cloud/region costs with the auto-configuring MACARON cache](#reducing-cross-cloudregion-costs-with-the-auto-configuring-macaron-cache)
  - [Table of Contents](#table-of-contents)
  - [Environment Setup](#environment-setup)
  - [Source Code](#source-code)
  - [Setup Instructions](#setup-instructions)

## Environment Setup

This code has been validated on AWS. However, as other cloud providers, such as Google Cloud Platform and Microsoft Azure, have similar environments, the code can be easily extended to support them as well.

## Source Code

The source code for the Macaron prototype is available at:
```console
git clone git@github.com:hojinp/macaron.git
```

The project is organized into the following directories:
- `data/`: Contains the data necessary to run the prototype, including latency files, Lambda zip files, and trace files.
- `src/`: Contains the implementation of the prototype.
- `scripts/`: Contains Python scripts for running the prototype.

## Setup Instructions

To run this project, additional preparation involving AMI, Amazon EFS, and AWS Lambda Function is required. The instructions below outline the process for running the Macaron prototype. Detailed guidance on configuring the AMI, EFS, and Lambda Function will be added soon. Please check back for updates.

## Instructions for running the prototype

This guide demonstrates how to run the IBM 58 workload on AWS in the us-east-1 region, with a data lake located in the us-west-1 region. This guide deploys cache cluster by default. Other workloads or configurations can be executed with minor adjustments to the settings described here.

1. **Preparing Data in the Data Lake for Running IBM 58.**

    In the AWS us-west-1 region, launch an instance with the r5.2xlarge instance type using the AMI ID `ami-01bc64231084c0220`.
    Build and install the Macaron prototype with the following steps:

    ```bash
    $ cd macaron
    $ mkdir build
    $ cd build
    $ cmake -DCMAKE_INSTALL_PREFIX=${HOME}/.macaron ..
    $ make
    $ make install
    ```

    Once Macaron is successfully installed, run the following script to store the necessary objects into the data lake:

    ```bash
    $ workload_prep --expname ibm058 --tracepath ${project_directory}/data/traces/IBMTrace058_0to6hr.bin --region us-west-1
    ```

    Delete this instance after the data lake is prepared.

2. **Running the Master node.**

    In the AWS us-east-1 region, launch an instance with the r5.2xlarge instance type using the AMI ID `ami-01bc64231084c0220`. Build and install the Macaron prototype following the same steps outlined in Step 1.

    After successfully installing Macaron, run the following script to launch the controller (MBakery) and the object storage cache manager (Meringue):

    ```bash
    $ python3 ${project_directory}/scripts/latency_validation/run_mbakery.py -n ibm058 -m ${private_ip_address_of_master_node}
    ```

    This script will launch a single-node cache cluster by default.

3. **Running the workload.**

    In the AWS us-east-1 region, launch an instance for the workload generator by following the same steps outlined in Steps 1 and 2.
    
    Run the following code to begin executing the trace and accessing remote data:

    ```bash
    $ python3 ${project_directory}/scripts/latency_validation/run_workload.py -n ibm058 -t ${project_directory}/data/traces/IBMTrace058_0to6hr.bin -m ${private_ip_address_of_master_node}
    ```
