#!/bin/bash
nosetests $*
# ignore tests code
pep8 evzookeeper | grep -v ^evzookeeper/tests
pyflakes evzookeeper | grep -v ^evzookeeper/tests
