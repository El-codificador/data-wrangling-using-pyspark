# Data Wrangling Using Pyspark
![cover](https://i0.wp.com/www.codeastar.com/wp-content/uploads/2017/07/data_wrangler.png)
## Project Aim and Objectives

___Aim:___
This project aims to type-cast and transform a log data using the “pyspark” module in Python.

___Objectives:___
- To access the zipped log data from a specified external source
- To extract the zipped data using the “tarfile” module
- To read the data using the “pyspark” module
- To transform the raw data using the “pyspark” module

## Data Description
The raw data, namely “BGL.log”, contains lines of information of system control failure prompts. 
![image](https://github.com/El-codificador/data-wrangling-using-pyspark/assets/91063835/5ebc6680-b83d-4e0c-82f9-cf56e2b781db)
<p><center>Figure 1: Snippet of raw data</center></p>

The segment of data shown in figure 1 contains the system failure prompts in the first line of which,
- “-” is the “Alert message flag” for a specific prompt
- “1117838570” is the “Timestamp” for a specific prompt
- “2005.06.03” is the “Date” on which a specific prompt is generated
- “R02-M1-N0-C:J12-U11” is the “Node” that is responsible for a particular prompt
- “2005-06-03 15:42:50.363779” is the date, in a typical date-time format, on which a specific prompt is thrown by a system
- “R02-M1-N0-C:J12-U11” is the confirmation of the “Node”, to be denoted as “Node (repeated), for which a system has encountered an error
- “RAS” is the “Message Type”
- “KERNEL” is the “System Component” that is associated with the error
- “INFO” is the “Level” of the error prompt
- “instruction cache parity error corrected” is the “Message Content”

Data are stored in the remaining lines of log in the same format. This data has to be transformed using the “pyspark” module so that it gets the following schema.

![image](https://github.com/El-codificador/data-wrangling-using-pyspark/assets/91063835/95e92408-8961-457c-ad54-07b543301b85)
<p><center>Figure 2: Dataframe schema</center></p>

The length of each of the line varies and needs to be oriented in ten columns where each line should represent each row in the pyspark dataframe.

## Problem Statement
This raw data could easily be restructured by separating each content (or word) of each line by the “space” character but this would also separate the “Message Contents” into more than one column like the following.
![image](https://github.com/El-codificador/data-wrangling-using-pyspark/assets/91063835/20c49cd2-dd19-486b-919b-55d1d2a2af5d)
![image](https://github.com/El-codificador/data-wrangling-using-pyspark/assets/91063835/7289f833-6258-45b8-89c4-0357096d642b)

Other than restructuring, the variables (or the columns) of the dataframe also need to be type-casted as the entire raw data is of “string” type (object type).

![image](https://github.com/El-codificador/data-wrangling-using-pyspark/assets/91063835/8d4d9cfa-3078-495a-b39d-e992a1352bb9)
<p><center>Figure 3: Diagrammatic representation of problem statement and solution</center></p>

The solution to the aforementioned problem has been provided in the following section.

## Solution

The “space” character has been used here as the delimiter to divide each line of the log data into ten substrings. From left to right the control flows and it defines each substring as the “word(s)” it encounters before a “space” character. Using the “getItem()” function the ten substrings are assigned to ten different columns.

![image](https://github.com/El-codificador/data-wrangling-using-pyspark/assets/91063835/c5b492aa-8de0-4785-a2de-6f89ceb67f37)
<p><center>Figure 4: Method employed for restructuring the data</center></p>

Here,

![image](https://github.com/El-codificador/data-wrangling-using-pyspark/assets/91063835/ceaeed94-d13e-433b-810e-c6ddc8b7fceb)

![image](https://github.com/El-codificador/data-wrangling-using-pyspark/assets/91063835/21358cb9-7c30-42fe-96a5-171766a6d5ca)
<p><center>Figure 5: Code explanation</center></p>

![image](https://github.com/El-codificador/data-wrangling-using-pyspark/assets/91063835/7d68b872-7c64-400c-a341-bdb90da9a300)
<p><center>Figure 6: Transformed data</center></p>

![image](https://github.com/El-codificador/data-wrangling-using-pyspark/assets/91063835/38746098-51b6-46ef-bc7d-aa56516ded68)
<p><center>Figure 7: Dataframe schema</center></p>
<hr>
## Cover Image Reference
I found this amazing art [here](https://www.codeastar.com/data-wrangling/).