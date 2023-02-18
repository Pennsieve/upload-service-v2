#! /bin/bash

echo ""
echo "**********************************"
echo "*   Testing MoveTrigger Lambda   *"
echo "**********************************"
echo ""
cd ./lambda/moveTrigger; \
  go test ./... ;
echo ""
echo "*******************************"
echo "*   Testing Service Lambda    *"
echo "*******************************"
echo ""
cd ../../lambda/service; \
  go test ./... ;
echo ""
echo "******************************"
echo "*   Testing Upload Lambda    *"
echo "******************************"
echo ""
cd ../../lambda/upload; \
  go test ./... ;
echo ""
echo "**********************************"
echo "*   Testing Move Fargate Task	*"
echo "**********************************"
echo ""
cd ../../fargate/upload-move; \
  go test ./... ;