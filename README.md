# COVID-19 Tweets Data Analysis Project using HBase

This project involves downloading a dataset of tweets related to COVID-19, inserting the data into HBase, and performing various data analysis tasks. The project is divided into several parts, each with specific objectives and tasks. Below is a detailed description of the steps taken and the tasks completed.

## Project Overview

1. **Data Download and Preparation**
2. **Data Insertion into HBase**
3. **Data Analysis Tasks**

## Data Download and Preparation

The dataset of tweets related to COVID-19 is downloaded from Kaggle. The link to the dataset is: [COVID-19 Tweets Dataset](https://www.kaggle.com/datasets/gpreda/covid19-tweets).

## Data Insertion into HBase

### HBase Table Structure

- **Column Families**:
  - **Users**: Contains user-related information.
  - **Tweets**: Contains tweet-related information.
  - **Extra**: Contains additional information.

Each row in the table has a unique row key.
### Task 1 
### Inserting Data

The data from the dataset is parsed and inserted into HBase under the three column families. Each entry in the dataset corresponds to a row in the HBase table.

### Task 2
### Versioning for "User Bio"

- **Create Versions**: Created versions for the column family that includes the "User Bio" column.
- **Insert Multiple Versions**: Inserted multiple data entries into "User Bio" for specific entries using the `Put` command.
- **Retrieve Versions**: Retrieved one or more versions of "User Bio" for the entries to verify that versioning works using the `Get` command.

### Task 3
### Tweets from Different Locations

- Counted the number of tweets from different locations by analyzing the "user_location" field.

### Task 4

#### Users Created by Year and Month

- **Yearly Creation**: Found the number of users created in each year.
- **Monthly Creation (2020)**: Found the number of users created in each month of 2020.
- **Verified Users**: Performed the same tasks for users who are verified.

### Task 5 

#### Users with Special Characters in Screen Name

- **All Users**: Identified users whose "Screen Name" includes special characters (other than alphabet and number).
- **Verified Users**: Performed the same task for users who are verified.

### Task 6

#### Popular Users

- **Popular Users**: Found the "Name" of users who have a "User Followers" count of more than six digits.
- **Verified vs Unverified**: Showed the number of verified popular users and unverified popular users.

### Task 7 

#### Tweets Starting with #covid19

- **All Tweets**: Found "Tweet Content" that starts with #covid19 (case insensitive).
- **Verified and Popular Users**: Performed the same task for users who are verified and popular.

## How to Use

1. **Download Dataset**: Download the dataset from [Kaggle](https://www.kaggle.com/datasets/gpreda/covid19-tweets).
2. **Insert Data into HBase**: Use the provided scripts to insert the data into HBase under the defined column families.
3. **Run Analysis Tasks**: Execute the scripts for each analysis task to get the desired results.

## Dependencies

- HBase
- Hadoop
- Eclipse IDE

Please follow the steps and instructions provided in this README file to successfully run and verify the data analysis tasks. 
