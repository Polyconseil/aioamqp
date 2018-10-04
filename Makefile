.PHONY: doc test update

PACKAGE = aioamqp

NOSETESTS ?= nosetests
TEST_OPTIONS ?= --verbosity=2
PYLINT_RC ?= .pylintrc

BUILD_DIR ?= build
INPUT_DIR ?= docs

# Sphinx options (are passed to build_docs, which passes them to sphinx-build)
#   -W       : turn warning into errors
#   -a       : write all files
#   -b html  : use html builder
#   -i [pat] : ignore pattern

SPHINXOPTS ?= -a -W -b html
AUTOSPHINXOPTS := -i *~ -i *.sw* -i Makefile*

SPHINXBUILDDIR ?= $(BUILD_DIR)/sphinx/html
ALLSPHINXOPTS ?= -d $(BUILD_DIR)/sphinx/doctrees $(SPHINXOPTS) docs

doc:
	sphinx-build -a $(INPUT_DIR) build

livehtml: docs
	sphinx-autobuild $(AUTOSPHINXOPTS) $(ALLSPHINXOPTS) $(SPHINXBUILDDIR)

test:
	$(NOSETESTS) $(PACKAGE) $(TEST_OPTIONS)


update:
	pip install -r requirements_dev.txt


### semi-private targets used by polyconseil's CI (copy-pasted from blease) ###

.PHONY: reports jenkins-test jenkins-quality

reports:
	mkdir -p reports

jenkins-test: reports
	$(MAKE) test TEST_OPTIONS="--with-coverage --cover-package=$(PACKAGE) \
		--cover-xml --cover-xml-file=reports/xmlcov.xml \
		--with-xunit --xunit-file=reports/TEST-$(PACKAGE).xml \
		-v \
		$(TEST_OPTIONS)"

jenkins-quality: reports
	pylint --rcfile=$(PYLINT_RC) $(PACKAGE) > reports/pylint.report || true
