#! /bin/bash
set -e

echo ""
echo "*******************************"
echo "*   Testing Service Lambda    *"
echo "*******************************"
echo ""
cd ./lambda/service; \
  go test -v ./... ;
echo ""
echo "******************************"
echo "*   Testing Upload Lambda    *"
echo "******************************"
echo ""
cd ../../lambda/upload; \
  go test -v ./... ;
echo ""
echo "******************************"
echo "*   Testing Archiver Lambda  *"
echo "******************************"
echo ""
cd ../../lambda/archiver; \
  go test -v ./... ;
echo ""
echo "*********************************"
echo "*   Testing Run Import Lambda   *"
echo "*********************************"
echo ""
cd ../../lambda/runimport; \
  go test -v ./... ;
echo ""
echo "*******************************"
echo "*   Testing pkg/runresults    *"
echo "*******************************"
echo ""
cd ../../pkg/runresults; \
  go test -v ./... ;
