.PHONY: run run-local

run:
	bash scripts/run_dev.sh

run-local:
	HOST=127.0.0.1 bash scripts/run_dev.sh
