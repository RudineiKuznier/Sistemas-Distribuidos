.PHONY: setup run stop clean

VENV = env
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip
HONCHO = $(VENV)/bin/honcho

setup:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

run:
	. $(VENV)/bin/activate && honcho start

clean: stop
	rm -rf $(VENV) __pycache__ *.pyc