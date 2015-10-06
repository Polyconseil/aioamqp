.PHONY: reset_rabbitmq test

NOSETESTS ?= nosetests

test:
	$(NOSETESTS) --verbosity=2 aioamqp


update:
	pip install -r requirements_dev.txt
