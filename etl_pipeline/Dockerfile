FROM python:3.10-slim

USER root
# Add repository code
WORKDIR /opt/dagster/app

COPY requirements.txt /opt/dagster/app

RUN pip install --upgrade pip && pip install -r requirements.txt

# WORKDIR /opt/dagster/app
# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository

EXPOSE 4000

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]
#CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]
