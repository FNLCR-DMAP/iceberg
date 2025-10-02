IMAGE?=iceberg-demo
PYTHON_VERSION?=3.9-slim
PLATFORM?=linux/amd64

.PHONY: build run shell clean warehouse

build:
	DOCKER_BUILDKIT=1 docker build --build-arg PYTHON_VERSION=$(PYTHON_VERSION) -t $(IMAGE) .

run:
	docker run --rm -v "$(PWD)/iceberg_warehouse:/app/iceberg_warehouse" $(IMAGE)

shell:
	docker run --rm -it -v "$(PWD)/iceberg_warehouse:/app/iceberg_warehouse" $(IMAGE) bash

clean:
	docker rmi $(IMAGE) || true

warehouse:
	rm -rf iceberg_warehouse && mkdir -p iceberg_warehouse
