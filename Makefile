.PHONY: help clean test package publish test-ci

LAMBDA_BUCKET ?= "pennsieve-cc-lambda-functions-use1"
WORKING_DIR   ?= "$(shell pwd)"
API_DIR ?= "api"
SERVICE_NAME  ?= "upload-service-v2"
SERVICE_PACKAGE_NAME ?= "upload-v2-service-${VERSION}.zip"
UPLOADHANDLER_PACKAGE_NAME ?= "upload-v2-handler-${VERSION}.zip"
MOVETRIGGER_PACKAGE_NAME ?= "upload-v2-move-trigger-${VERSION}.zip"
PACKAGE_NAME  ?= "${SERVICE_NAME}-${VERSION}.zip"

.DEFAULT: help

help:
	@echo "Make Help for $(SERVICE_NAME)"
	@echo ""
	@echo "make clean   - removes node_modules directory"
	@echo "make test    - run tests"
	@echo "make package - create venv and package lambdas and fargate functions"
	@echo "make publish - package and publish lambda function"

test:
	docker-compose -f docker-compose.test.yml down --remove-orphans
	docker-compose -f docker-compose.test.yml up --exit-code-from local_tests local_tests

test-ci:
	mkdir -p test-dynamodb-data
	chmod -R 777 test-dynamodb-data
	docker-compose -f docker-compose.test.yml down --remove-orphans
	docker-compose -f docker-compose.test.yml up --exit-code-from ci_tests ci_tests

# Start a clean DynamoDB container for local testing
start-dynamodb: docker-clean
	docker-compose -f docker-compose.test.yml up dynamodb


# Spin down active docker containers.
docker-clean:
	docker-compose -f docker-compose.test.yml down

# Remove dynamodb database
clean: docker-clean
	rm -rf test-dynamodb-data

package:
	@echo ""
	@echo "***********************"
	@echo "*   Building Service lambda   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/lambda/service; \
  		env GOOS=linux GOARCH=amd64 go build -o $(WORKING_DIR)/lambda/bin/service/pennsieve_upload_service; \
		cd $(WORKING_DIR)/lambda/bin/service/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/service/$(SERVICE_PACKAGE_NAME) .
	@echo ""
	@echo "***********************"
	@echo "*   Building Upload lambda   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/lambda/upload; \
		env GOOS=linux GOARCH=amd64 go build -o $(WORKING_DIR)/lambda/bin/upload/pennsieve_upload_handler; \
		cd $(WORKING_DIR)/lambda/bin/upload/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/upload/$(UPLOADHANDLER_PACKAGE_NAME) .
	@echo ""
	@echo "***********************"
	@echo "*   Building Move Trigger lambda   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/lambda/moveTrigger; \
  		env GOOS=linux GOARCH=amd64 go build -o $(WORKING_DIR)/lambda/bin/moveTrigger/pennsieve_move_trigger; \
		cd $(WORKING_DIR)/lambda/bin/moveTrigger/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/moveTrigger/$(MOVETRIGGER_PACKAGE_NAME) .
	@echo ""
	@echo "***********************"
	@echo "*   Building Fargate   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/fargate/upload-move; \
#		env GOOS=linux GOARCH=amd64 go build -o app/upload-move-files; \
		docker build -t pennsieve/upload_move_files:${VERSION} . ;\
		docker push pennsieve/upload_move_files:${VERSION} ;\

publish:
	@make package
	@echo ""
	@echo "*************************"
	@echo "*   Publishing Service lambda   *"
	@echo "*************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/service/$(SERVICE_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/upload-service-v2/service/
	rm -rf $(WORKING_DIR)/lambda/bin/service/$(SERVICE_PACKAGE_NAME)
	@echo ""
	@echo "*************************"
	@echo "*   Publishing Upload lambda   *"
	@echo "*************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/upload/$(UPLOADHANDLER_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/upload-service-v2/upload/
	rm -rf $(WORKING_DIR)/lambda/bin/upload/$(UPLOADHANDLER_PACKAGE_NAME)
	@echo ""
	@echo "*************************"
	@echo "*   Publishing Move Trigger lambda   *"
	@echo "*************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/moveTrigger/$(MOVETRIGGER_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/upload-service-v2/trigger/
	rm -rf $(WORKING_DIR)/lambda/bin/moveTrigger/$(MOVETRIGGER_PACKAGE_NAME)
