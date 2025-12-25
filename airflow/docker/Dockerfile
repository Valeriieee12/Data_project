FROM apache/airflow:2.7.1-python3.10

USER root

# Устанавливаем Java (необходим для PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    wget \
    vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем переменные окружения для Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Копируем PostgreSQL драйвер для Spark
COPY files/postgresql-42.7.0.jar /opt/airflow/postgresql-42.7.0.jar
COPY files/postgresql-42.7.0.jar /opt/spark/jars/

# Создаем директорию для данных и скриптов
RUN mkdir -p /opt/airflow/data /opt/airflow/scripts /opt/airflow/logs

# Устанавливаем права на директории
RUN chown -R airflow:airflow /opt/airflow /opt/spark/jars

# Меняем пользователя обратно на airflow
USER airflow

# Копируем requirements и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# Копируем скрипты (будет выполнено позже через volumes в docker-compose)
# COPY scripts /opt/airflow/scripts/
