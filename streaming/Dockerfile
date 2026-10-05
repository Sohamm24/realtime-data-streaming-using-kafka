FROM apache/spark:3.5.1

USER root

# Install Python packages required for Delta Lake, DuckDB, and unit tests
RUN pip3 install --no-cache-dir delta-spark==3.2.0 duckdb==1.0.0 pytest==8.3.5

WORKDIR /app

# Configure permissions for the unprivileged spark user
RUN mkdir -p /app/data /home/spark/.ivy2 /home/spark/.cache && \
    chown -R spark:spark /app /home/spark

USER spark

COPY schemas.py transforms.py stream_job.py /app/

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[2]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0", \
     "--conf", "spark.driver.memory=2g", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "/app/stream_job.py"]
