.PHONY: help clean test package publish test-ci

LAMBDA_BUCKET ?= "pennsieve-cc-lambda-functions-use1"
WORKING_DIR   ?= "$(shell pwd)"
API_DIR ?= "api"
SERVICE_NAME  ?= "upload-service-v2"
SERVICE_PACKAGE_NAME ?= "upload-v2-service-${VERSION}.zip"
UPLOADHANDLER_PACKAGE_NAME ?= "upload-v2-handler-${VERSION}.zip"
ARCHIVER_PACKAGE_NAME ?= "manifest-archiver-${VERSION}.zip"
RECONCILE_PACKAGE_NAME ?= "upload-v2-reconcile-${VERSION}.zip"
ARCHIVE_SWEEPER_PACKAGE_NAME ?= "upload-v2-archive-sweeper-${VERSION}.zip"
RUNIMPORT_PACKAGE_NAME ?= "upload-v2-runimport-${VERSION}.zip"
PACKAGE_NAME  ?= "${SERVICE_NAME}-${VERSION}.zip"

.DEFAULT: help

help:
	@echo "Make Help for $(SERVICE_NAME)"
	@echo ""
	@echo "make clean   - removes node_modules directory"
	@echo "make test    - run tests"
	@echo "make package - create venv and package lambdas"
	@echo "make publish - package and publish lambda function"

test: clean
	docker compose -f docker-compose.test.yml down --remove-orphans
	docker compose -f docker-compose.test.yml up --build --exit-code-from local_tests local_tests

test-ci:
	mkdir -p test-dynamodb-data
	chmod -R 777 test-dynamodb-data
	mkdir -p testdata
	chmod -R 777 testdata
	docker compose -f docker-compose.test.yml down --remove-orphans
	docker compose -f docker-compose.test.yml up --build --exit-code-from ci_tests ci_tests

go-get:
	cd $(WORKING_DIR)/lambda/service; \
		go get github.com/pennsieve/pennsieve-upload-service-v2/service
	cd $(WORKING_DIR)/lambda/upload; \
		go get github.com/pennsieve/pennsieve-upload-service-v2/upload
	cd $(WORKING_DIR)/lambda/archiver; \
		go get github.com/pennsieve/pennsieve-upload-service-v2/archiver

# Spin down active docker containers.
docker-clean:
	docker compose -f docker-compose.test.yml down

# Remove dynamodb database
clean: docker-clean
	rm -rf test-dynamodb-data
	rm -rf testdata
	rm -rf lambda/bin
	rm -rf miniodata

package:
	@echo ""
	@echo "***********************"
	@echo "*   Building Service lambda   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/lambda/service; \
  		env GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o $(WORKING_DIR)/lambda/bin/service/bootstrap; \
		cd $(WORKING_DIR)/lambda/bin/service/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/service/$(SERVICE_PACKAGE_NAME) .
	@echo ""
	@echo "***********************"
	@echo "*   Building Upload lambda   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/lambda/upload; \
		env GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o $(WORKING_DIR)/lambda/bin/upload/bootstrap; \
		cd $(WORKING_DIR)/lambda/bin/upload/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/upload/$(UPLOADHANDLER_PACKAGE_NAME) .
	@echo ""
	@echo "***********************"
	@echo "*   Building Manifest Archiver lambda   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/lambda/archiver; \
  		env GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o $(WORKING_DIR)/lambda/bin/archiver/bootstrap; \
		cd $(WORKING_DIR)/lambda/bin/archiver/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/archiver/$(ARCHIVER_PACKAGE_NAME) .
	@echo ""
	@echo "***********************"
	@echo "*   Building Reconcile lambda   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/lambda/reconcile; \
  		env GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o $(WORKING_DIR)/lambda/bin/reconcile/bootstrap; \
		cd $(WORKING_DIR)/lambda/bin/reconcile/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/reconcile/$(RECONCILE_PACKAGE_NAME) .
	@echo ""
	@echo "***********************"
	@echo "*   Building Archive Sweeper lambda   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/lambda/archive-sweeper; \
  		env GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o $(WORKING_DIR)/lambda/bin/archive-sweeper/bootstrap; \
		cd $(WORKING_DIR)/lambda/bin/archive-sweeper/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/archive-sweeper/$(ARCHIVE_SWEEPER_PACKAGE_NAME) .
	@echo ""
	@echo "***********************"
	@echo "*   Building Run Import lambda   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/lambda/runimport; \
  		env GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o $(WORKING_DIR)/lambda/bin/runimport/bootstrap; \
		cd $(WORKING_DIR)/lambda/bin/runimport/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/runimport/$(RUNIMPORT_PACKAGE_NAME) .

publish:
	@make package
	@echo ""
	@echo "*********************************"
	@echo "*   Publishing Service lambda   *"
	@echo "*********************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/service/$(SERVICE_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/upload-service-v2/service/
	rm -rf $(WORKING_DIR)/lambda/bin/service/$(SERVICE_PACKAGE_NAME)
	@echo ""
	@echo "********************************"
	@echo "*   Publishing Upload lambda   *"
	@echo "********************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/upload/$(UPLOADHANDLER_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/upload-service-v2/upload/
	rm -rf $(WORKING_DIR)/lambda/bin/upload/$(UPLOADHANDLER_PACKAGE_NAME)
	@echo ""
	@echo "************************************"
	@echo "*   Publishing Manifest Archiver   *"
	@echo "************************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/archiver/$(ARCHIVER_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/upload-service-v2/archiver/
	rm -rf $(WORKING_DIR)/lambda/bin/archiver/$(ARCHIVER_PACKAGE_NAME)
	@echo ""
	@echo "************************************"
	@echo "*   Publishing Reconcile lambda    *"
	@echo "************************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/reconcile/$(RECONCILE_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/upload-service-v2/reconcile/
	rm -rf $(WORKING_DIR)/lambda/bin/reconcile/$(RECONCILE_PACKAGE_NAME)
	@echo ""
	@echo "*****************************************"
	@echo "*   Publishing Archive Sweeper lambda   *"
	@echo "*****************************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/archive-sweeper/$(ARCHIVE_SWEEPER_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/upload-service-v2/archive-sweeper/
	rm -rf $(WORKING_DIR)/lambda/bin/archive-sweeper/$(ARCHIVE_SWEEPER_PACKAGE_NAME)
	@echo ""
	@echo "*************************************"
	@echo "*   Publishing Run Import lambda    *"
	@echo "*************************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/runimport/$(RUNIMPORT_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/upload-service-v2/runimport/
	rm -rf $(WORKING_DIR)/lambda/bin/runimport/$(RUNIMPORT_PACKAGE_NAME)
