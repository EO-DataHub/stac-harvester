# syntax=docker/dockerfile:1
FROM python:3.12-slim-bullseye

# Set environment variables for OpenTelemetry
ENV OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true
ENV OTEL_SERVICE_NAME=stac-harvester
ENV OTEL_TRACES_EXPORTER=console
ENV OTEL_METRICS_EXPORTER=console
ENV OTEL_LOGS_EXPORTER=console
ENV OTEL_LOG_LEVEL=debug

RUN rm -f /etc/apt/apt.conf.d/docker-clean; \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update -y && apt-get upgrade -y \
    && apt-get install --yes --quiet git 
RUN apt-get update && apt-get install -y curl
RUN curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl"
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin/kubectl


WORKDIR /stac_harvester
ADD LICENSE requirements.txt ./
ADD stac_harvester ./stac_harvester/
ADD pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip pip3 install -r requirements.txt .

ENTRYPOINT ["python", "-m", "stac_harvester"]