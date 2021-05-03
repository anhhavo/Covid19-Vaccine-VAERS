import os
import base64
import plotly
import plotly.express as px
from io import BytesIO
#from flask import Flask, render_template, jsonify, send_file
#from flask_socketio import SocketIO, emit, send
import pandas as pd
#import matplotlib.pyplot as plt
#import jinja2
import streamlit as st

#st.set_page_config(layout="wide")

@st.cache #Load data from CSV
def load_data(nrows,filepath):
    data = pd.read_csv(filepath, encoding = "ISO-8859-1", nrows = nrows)
    return data

pplinEachAgeGroup = pd.read_csv('s3://cse427-finalproject/Results/pplInEachAgeGroup.csv')
ppl_each_vaccine = pd.read_csv('s3://cse427-finalproject/Results/ppl_each_vaccine.csv')
ppl_each_vaccine = ppl_each_vaccine.drop(['Unnamed: 0'],axis=1)
ppl_each_vaccine=ppl_each_vaccine.set_index("VAX_MANU")
peopledied_per_vaccine = pd.read_csv('s3://cse427-finalproject/Results/peopledied_per_vaccine.csv')

peopledied_per_vaccine.rename(columns = {'count(DISTINCT VAERS_ID)' : 'Number of people'}, inplace = True)
peopledied_per_vaccine = peopledied_per_vaccine.drop(['Unnamed: 0'],axis=1)
peopledied_per_vaccine = peopledied_per_vaccine.drop(['Unnamed: 0.1'],axis=1)
recov_death = pd.read_csv('s3://cse427-finalproject/Results/recovd_death.csv')
recov_death.rename(columns = {'Unnamed: 0' : 'Status'}, inplace = True)


st.title("Covid Vaccine Analysis Website")
#for exta spacing
st.text("")
st.text("")
#info:
st.write('''
Our data is chosen from Vaccine Adverse Event Reporting System (VAERS).
We want to analyze data from people who took COVID-19 Vaccines that is reported to VAERS.
This will help us understand how effectiveness of COVID-19 Vaccine from different manufacturer.
''')

st.header('General Vaccine Information')
st.subheader('Number of Report From Each Vaccine')
st.write('''
There are 3 Companies that provide COVID-19 Vaccines. They are Moderna, Janssen, Pfizer\Biontech.
The amount of people who took COVID-19 vaccines from each manufacturers based on the reports are presented in this graph.
''')
st.bar_chart(ppl_each_vaccine)

st.markdown("***")

st.subheader('Age group division')
st.text("")
st.write('''
We divided the data in 4 age groups: Children (0-14), Youth (15-24), Adults (25-64), Seniors (65+)
''')
pplinEachAgeGroup

st.subheader("Report of Death")
st.write(''' The number of people who died after taking COVID Vaccine ''')
peopledied_per_vaccine
st.subheader("Recovered or Dead?")
st.write(''' Some people who took COVID-19 Vaccine were already under some life threating illnesses. We decided to run two queries to findout how many has recovered with life threatening illness after taking COVID-19. And how many had life threatening illness and died? Here is the answer , ''')
recov_death



modernaSymptomsCount = load_data(30,'s3://cse427-finalproject/Results/modernaSymptomsCount.csv')
pfizerSymptomsCount = load_data(30, 's3://cse427-finalproject/Results/pfizerSymptomsCount.csv')
JANSSENSymptomsCount = load_data(30, 's3://cse427-finalproject/Results/JANSSENSymptomsCount.csv')
modernaSymptomsCountChild = load_data(10, 's3://cse427-finalproject/Results/modernaSymptomsCountChild.csv')
modernaSymptomsCountYouth = load_data(30, 's3://cse427-finalproject/Results/modernaSymptomsCountYouth.csv')
modernaSymptomsCountAdult = load_data(30, 's3://cse427-finalproject/Results/modernaSymptomsCountAdult.csv')
modernaSymptomsCountSenior = load_data(30, 's3://cse427-finalproject/Results/modernaSymptomsCountSenior.csv')
pfizerSymptomsCountChild = load_data(10, 's3://cse427-finalproject/Results/pfizerSymptomsCountChild.csv')
pfizerSymptomsCountYouth = load_data(30, 's3://cse427-finalproject/Results/pfizerSymptomsCountYouth.csv')
pfizerSymptomsCountAdult = load_data(30, 's3://cse427-finalproject/Results/pfizerSymptomsCountAdult.csv')
pfizerSymptomsCountSenior = load_data(30, 's3://cse427-finalproject/Results/pfizerSymptomsCountSenior.csv')
JANSSENSymptomsCountChild = load_data(10, 's3://cse427-finalproject/Results/JANSSENSymptomsCountChild.csv')
JANSSENSymptomsCountYouth = load_data(30, 's3://cse427-finalproject/Results/JANSSENSymptomsCountYouth.csv')
JANSSENSymptomsCountAdult = load_data(30, 's3://cse427-finalproject/Results/JANSSENSymptomsCountAdult.csv')
JANSSENSymptomsCountSenior = load_data(30, 's3://cse427-finalproject/Results/JANSSENSymptomsCountSenior.csv')

print(modernaSymptomsCountChild)
st.markdown("***")
st.header('Symptom Information: Most Frequent Symptoms')
age_group_options = ['all age groups', 'children', 'youth', 'adults', 'seniors']
age_group = st.selectbox("Choose to display vaccine data for a specific age group: ", options = age_group_options)

plots = {
    "all age groups": [modernaSymptomsCount,pfizerSymptomsCount,JANSSENSymptomsCount],
    "children": [modernaSymptomsCountChild,pfizerSymptomsCountChild,JANSSENSymptomsCountChild],
    "youth": [modernaSymptomsCountYouth,pfizerSymptomsCountYouth,JANSSENSymptomsCountYouth],
    "adults": [modernaSymptomsCountAdult,pfizerSymptomsCountAdult,JANSSENSymptomsCountAdult],
    "seniors": [modernaSymptomsCountSenior,pfizerSymptomsCountSenior,JANSSENSymptomsCountSenior]
}

# if age_group == "all age groups":
fig = px.bar(
    x=plots[age_group][0]['freq'],
    y=plots[age_group][0]['SYMPTOM1'],
    orientation='h',
    height=600,
    )
fig.update_layout(
    yaxis={'categoryorder':'max ascending'},
    xaxis_title="Symptom Frequency Out Of All Symptoms",
    yaxis_title="COVID-19 Moderna Vaccine Related Symptoms",
    title_text='Moderna', title_x=0.4
    )
fig1 = px.bar(
    x=plots[age_group][1]['freq'],
    y=plots[age_group][1]['SYMPTOM1'],
    orientation='h',
    height=600,
    )
fig1.update_layout(
    yaxis={'categoryorder':'max ascending'},
    xaxis_title="Symptom Frequency Out Of All Symptoms",
    yaxis_title="COVID-19 PFIZER\BIONTECH Vaccine Related Symptoms",
    title_text='Pfizer\Biontech', title_x=0.4
    )
fig2 = px.bar(
    x=plots[age_group][2]['freq'],
    y=plots[age_group][2]['SYMPTOM1'],
    orientation='h',
    height=600,
    )
fig2.update_layout(
    yaxis={'categoryorder':'max ascending'},
    xaxis_title="Symptom Frequency Out Of All Symptoms",
    yaxis_title="COVID-19 JANSSEN Vaccine Related Symptoms",
    title_text='JANSSEN', title_x=0.4
    )

st.write(fig)
st.write(fig1)
st.write(fig2)
# plots = {
#     "all age groups": [fig,fig1,fig2],
#     "Second chart": 1,
#     "Third chart": 2
# }
#st.write(plots[age_group][0], plots[age_group][1], plots[age_group][2])
