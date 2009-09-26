PYTHON=/usr/bin/python
PYTHONPATH=.

all: test

test:
	PYTHONPATH=${PYTHONPATH} ${PYTHON} tests/basic.py
