# Covid Vaccine Analysis Website 

Members:
Anh Ha Vo (465820)
Grace Chen (457001)
Yiqing Zhang (464515)

Here is the [link](http://3.140.146.214:8501/) to our website.

Here is the [link](https://docs.google.com/presentation/d/1ZDso8ozNYUeNIeNqH7aPHa4RfmYP0OogNlmidDm01z4/edit#slide=id.gd5d4def508_0_565) to our presentation:

# Description:
We were interested in analying COVID19 data especially when many people are now taking Covid-Vaccine. We want to analyze data from people who took COVID-19 Vaccines that is reported to VAERS. Our data is chosen from Vaccine Adverse Event Reporting System (VAERS) https://vaers.hhs.gov/

# Data Analysis
We uploaded the three CSV files into AWS S3. On the Splice Machine server, we extracted the CSV files into pandas DataFrames and converted them all to spark DataFrames. We then run multiple spark SQL queries on the data, e.g. what adverse events happened most often for youth taking the Moderna vaccine. We then stored all the query results back into AWS S3.
All the queries can be found in **src/Spark-queries.py**

# Data Visualization
We designed a web page to present our query results using Streamlit. We added several interactive features. There is a zoom in/zoom out button for all the graphs and a drop-down list to select by age group. There is also a tool bar next to each plot in “Symptom Information: Most Frequent Symptoms” where users can download the plot, draw Lasso Select, etc. Users can also mouse over each bar on all the graphs to read more statistical values.
All the visualization can be found in **src/app.py**

# Docker
We dockerized our website into an image and pushed it to the docker hub. The image is at **anhhavo/cse427-finalproject:latest2**\
To install docker, follow this [documentation](https://docs.google.com/presentation/d/1ZAFXAtPQ4YvD2UwwhlDhSmoT0zgeY0sMVansX-XjlbQ/edit?usp=sharing)

# Kubernetes
We deployed our image into Minikube with ingress on the local machine. The website can be accessed at **covidvaccine-vaers.info** locally.
To install Kubernetes, type in terminal "brew install minikube"

# Room for future improvements
- Coming soon.

# To run our app:
- docker ps, if you see minikube, type "minikube delete"
- minikube start --vm=true
- minikube addons enable ingress
- kubectl apply -f charts
- sudo vi /etc/hosts -> add host "covidvaccine-vaers.info" to the corresponding ingress address
- whenever the deployment is ready, website will be up and running!
