.PHONY: dockerbuild dockerpush test testonce ruff black lint isort pre-commit-check setup
VERSION ?= latest
IMAGENAME = stac-harvester
DOCKERREPO ?= public.ecr.aws/n1b3o1k2/ukeodhp

dockerbuild:
	DOCKER_BUILDKIT=1 docker build -t ${IMAGENAME}:${VERSION} .

dockerpush: dockerbuild testdocker
	docker tag ${IMAGENAME}:${VERSION} ${DOCKERREPO}/${IMAGENAME}:${VERSION}
	docker push ${DOCKERREPO}/${IMAGENAME}:${VERSION}

test:
	uv run ptw stac-harvester

testonce:
	uv run pytest

ruff:
	uv run ruff check .

black:
	uv run black .

isort:
	uv run isort . --profile black

validate-pyproject:
	validate-pyproject pyproject.toml

lint: ruff black isort validate-pyproject

.git/hooks/pre-commit:
	uv run pre-commit install
	curl -o .pre-commit-config.yaml https://raw.githubusercontent.com/EO-DataHub/github-actions/main/.pre-commit-config-python.yaml

setup: .git/hooks/pre-commit
	uv sync
