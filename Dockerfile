# syntax=docker/dockerfile:1
FROM python:3.11-slim-bullseye

RUN rm -f /etc/apt/apt.conf.d/docker-clean; \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update -y && apt-get upgrade -y \
    && apt-get install -y git

WORKDIR /stac_harvester
ADD LICENSE requirements.txt ./
ADD stac_harvester ./stac_harvester/
ADD pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip pip3 install -r requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip pip3 install "git+https://github.com/UKEODHP/configscanning.git@v0.1.3"

CMD python -m stac_harvester $1 $2 $3 $4
