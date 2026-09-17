FROM apache/airflow:2.9.3-python3.9

USER root

# Java 11 설치 (PySpark 필수)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Python 의존성 설치
# requirements.txt 가 아니라 airflow_requirements.txt 를 쓴다 — 이유는 그 파일 주석 참조.
# (공식 constraint 파일은 pyspark==3.5.1 을 강제해서 이 저장소가 고정한 3.3.4 와 충돌한다.
#  SQLAlchemy 를 깨뜨리던 원인이 sqlalchemy>=2.0.0 하나뿐이라 그걸 빼는 것으로 충분하다)
COPY airflow_requirements.txt /tmp/airflow_requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow_requirements.txt
