FROM apache/airflow:2.1.1

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow
# WORKDIR /app
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt
RUN rm -rf requirements.txt


# USER root
# COPY ../spark/jars/* .

# RUN mkdir 
# RUN mkdir -p /opt/jars
# RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.2/hadoop-aws-3.1.2.jar --output /opt/jars/hadoop-aws-3.1.2.jar
# RUN curl -L https://repos.spark-packages.org/saurfang/spark-sas7bdat/3.0.0-s_2.12/spark-sas7bdat-3.0.0-s_2.12.jar --output /opt/jars/spark-sas7bdat-3.0.0-s_2.12.jar
# RUN curl -L https://repo1.maven.org/maven2/com/epam/parso/2.0.11/parso-2.0.11.jar  --output /opt/jars/parso-2.0.11.jar
# RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar --output /opt/jars/aws-java-sdk-bundle-1.11.271.jar
# RUN chmod -R 777 *.jar
# WORKDIR /opt/airflow/dags


