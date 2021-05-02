FROM python:3.8.5
# streamlit-specific commands
RUN mkdir -p /root/.streamlit
RUN bash -c 'echo -e "\
[general]\n\
email = \"\"\n\
" > /root/.streamlit/credentials.toml'
RUN bash -c 'echo -e "\
[server]\n\
enableCORS = false\n\
" > /root/.streamlit/config.toml'

COPY conf /tmp 
RUN pip3 install pandas 
RUN pip3 install --upgrade pip
RUN pip3 install --upgrade setuptools
RUN pip3 install --no-cache-dir -U pip wheel setuptools
RUN pip3 install numpy
RUN pip3 install streamlit
RUN pip3 install plotly
RUN pip3 install boto3
RUN pip3 install fsspec
RUN pip3 install s3fs



# exposing default port for streamlit
EXPOSE 8080

WORKDIR /code
COPY src /code
CMD streamlit run app.py

kops create cluster \
--state=${KOPS_STATE_STORE} \
--node-count=2 \
--master-size=t3.micro \
--node-size=t3.micro \
--zones=us-west-1a,us-west-1b \
--name=${KOPS_CLUSTER_NAME} \
--dns private \
--master-count 1
