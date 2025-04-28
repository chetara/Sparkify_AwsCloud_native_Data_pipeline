# Use an official Spark base image with Python support
FROM bitnami/spark:3.1.2

# Switch to root to install pip packages
USER root

# Install and upgrade pip
RUN pip install --upgrade pip

# Install necessary Python packages
RUN pip install boto3 pyspark

# Install google-re2
RUN pip install google-re2

# Set the environment variable for UTF-8 encoding
ENV LANG C.UTF-8


# Set environment variables for Spark
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Set working directory to /app inside the container
WORKDIR /app

# Copy the spark_etl.py job to the container
COPY spark_jobs/spark_etl.py /app/spark_etl.py

# Set the entry point for the container (runs the Spark ETL job)
ENTRYPOINT ["python", "spark_etl.py"]
