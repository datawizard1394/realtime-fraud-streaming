.PHONY: help test check demo clean docker-demo

PYTHON ?= python3
PYTHONPATH := src

help:
	@echo "Targets:"
	@echo "  make test        Run the dependency-free unit and integration tests"
	@echo "  make check       Compile every Python module, then run tests"
	@echo "  make demo        Generate synthetic events, alerts, and metrics"
	@echo "  make docker-demo Run the same demo in a disposable container"
	@echo "  make clean       Remove generated local artifacts"

test:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m unittest discover -s tests -v

check:
	$(PYTHON) -m compileall -q src tests
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m unittest discover -s tests -v

demo:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m fraud_streaming \
		--config config/rules.json \
		--events 100 \
		--seed 42 \
		--output-dir artifacts

docker-demo:
	docker compose run --rm demo

clean:
	$(PYTHON) -c "import pathlib, shutil; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('.').rglob('__pycache__')]; shutil.rmtree('artifacts', ignore_errors=True)"

