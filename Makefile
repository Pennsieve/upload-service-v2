.PHONY: help clean test package publish

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
	@echo ""
	@echo "**********************************"
	@echo "*   Testing MoveTrigger Lambda   *"
	@echo "**********************************"
	@echo ""
	@cd $(WORKING_DIR)/lambda/moveTrigger; \
		go test ./... ;
	@echo ""
	@echo "*******************************"
	@echo "*   Testing Service Lambda    *"
	@echo "*******************************"
	@echo ""
	@cd $(WORKING_DIR)/lambda/service; \
		go test ./... ;
	@echo ""
	@echo "******************************"
	@echo "*   Testing Upload Lambda    *"
	@echo "******************************"
	@echo ""
	@cd $(WORKING_DIR)/lambda/upload; \
		go test ./... ;
	@echo ""
	@echo "**********************************"
	@echo "*   Testing Move Fargate Task	*"
	@echo "**********************************"
	@echo ""
	@cd $(WORKING_DIR)/fargate/upload-move; \
		go test ./... ;

package:
	@echo ""
	@echo "***********************"
	@echo "*   Building Service lambda   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/lambda/service; \
  		env GOOS=linux GOARCH=amd64 go build -o $(WORKING_DIR)/lambda/bin/service/$(SERVICE_NAME)-$(VERSION); \
		cd $(WORKING_DIR)/lambda/bin/service/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/service/$(SERVICE_PACKAGE_NAME) .
	@echo ""
	@echo "***********************"
	@echo "*   Building Upload lambda   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/lambda/service; \
		env GOOS=linux GOARCH=amd64 go build -o $(WORKING_DIR)/lambda/bin/upload/$(SERVICE_NAME)-$(VERSION); \
		cd $(WORKING_DIR)/lambda/bin/upload/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/upload/$(UPLOADHANDLER_PACKAGE_NAME) .
	@echo ""
	@echo "***********************"
	@echo "*   Building Move Trigger lambda   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/lambda/service; \
  		env GOOS=linux GOARCH=amd64 go build -o $(WORKING_DIR)/lambda/bin/moveTrigger/$(SERVICE_NAME)-$(VERSION); \
		cd $(WORKING_DIR)/lambda/bin/moveTrigger/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/moveTrigger/$(MOVETRIGGER_PACKAGE_NAME) .
	@echo ""
	@echo "***********************"
	@echo "*   Building Fargate   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/fargate/upload-move; \
		env GOOS=linux GOARCH=amd64 go build -o $(WORKING_DIR)/fargate/bin/uploadMove/$(SERVICE_NAME)-$(VERSION); \
		docker buildx build --platform linux/amd64 -t pennsieve/upload_move_files:${VERSION} -t pennsieve/upload_move_files:latest . ;\
		docker push pennsieve/upload_move_files:${VERSION}
		docker push pennsieve/upload_move_files:latest

publish:
	@make package
	@echo ""
	@echo "*************************"
	@echo "*   Publishing Service lambda   *"
	@echo "*************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/service/$(SERVICE_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/$(SERVICE_PACKAGE_NAME)/
	rm -rf $(WORKING_DIR)/lambda/bin/service/$(SERVICE_PACKAGE_NAME)
	@echo ""
	@echo "*************************"
	@echo "*   Publishing Upload lambda   *"
	@echo "*************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/upload/$(UPLOADHANDLER_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/$(UPLOADHANDLER_PACKAGE_NAME)/
	rm -rf $(WORKING_DIR)/lambda/bin/upload/$(UPLOADHANDLER_PACKAGE_NAME)
	@echo ""
	@echo "*************************"
	@echo "*   Publishing Move Trigger lambda   *"
	@echo "*************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/moveTrigger/$(MOVETRIGGER_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/$(MOVETRIGGER_PACKAGE_NAME)/
	rm -rf $(WORKING_DIR)/lambda/bin/moveTrigger/$(MOVETRIGGER_PACKAGE_NAME)
