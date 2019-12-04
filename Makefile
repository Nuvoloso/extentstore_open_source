# Copyright 2019 Tad Lebeck
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# add vendor/glide later
.PHONY: all vendor build depend copy_certs install_config_certs jsonlint clean veryclean test

# variables needed for managing certs
CP=cp
CERTS=certs
TOOLS_REPO=$(HOME)/src/tools
TOOLS_CERTS=$(TOOLS_REPO)/$(CERTS)
CA_CRT=ca.crt
ETC_NUVOLOSO=/etc/nuvoloso

all: build

BUILD_DIRS=\
	copy \
	mdDecode \
	mdEncode \
	parallelcopy

build:: depend copy_certs jsonlint
	@for d in $(BUILD_DIRS); do echo "** Build directory $$d"; $(MAKE) -C $$d build || exit 1; done

clean::
	for d in $(BUILD_DIRS); do $(MAKE) -C $$d clean; done

# make a local copy of the certs
copy_certs:
	@if [ ! -d $(CERTS) ]; then \
		echo "** Copy certificates" ; \
		$(CP) -r $(TOOLS_CERTS) $(CERTS); \
	fi

# install all required certs/config files into /etc directory
# TBD: change to use install
install_config_certs:

# test assumes the vendor directory is present
TEST_DIRS=

TEST_FLAGS=-coverprofile cover.out -timeout 30s
EXTRA_TEST_FLAGS=
test::
	@TOP=$$(pwd); for d in $(TEST_DIRS); do cd $$TOP/$$d; echo "** Test directory $$d"; go test $(TEST_FLAGS) $(EXTRA_TEST_FLAGS) || exit 1 ; done
	@echo Use \"go tool cover -html=cover.out -o=cover.html\" to view output in each directory

jsonlint::
	find deploy -name '*.json' | xargs ./vendor/github.com/Nuvoloso/kontroller/jsonlint.py

# depend will not re-create the vendor directory
VENDOR_TGT=.vendor
depend:: $(VENDOR_TGT)
# vendor will re-create the vendor directory
vendor:
	$(RM) -r vendor $(VENDOR_TGT)
	$(MAKE) $(VENDOR_TGT)
$(VENDOR_TGT): 
$(VENDOR_TGT): glide.yaml
	glide install -v || (glide cc && glide install -v)
	touch $(VENDOR_TGT)
# don't clean vendor stuff by default
veryclean:: clean
	$(RM) -r vendor $(VENDOR_TGT)

# CONTAINER_TAG can be specified on the command line or in the environment to override this default
CONTAINER_TAG?=latest
container_build:

container_push:

container: container_build container_push

