#!/usr/bin/env bash
set -e

cd ${GOPATH}/src/github.com/kubernetes-sigs/alibaba-cloud-csi-driver/build/ossglobal/
GIT_SHA=`git rev-parse --short HEAD || echo "HEAD"`

export GOARCH="amd64"
export GOOS="linux"

branch="v1.0.0"
version="v1.14.8"
commitId=$GIT_SHA
buildTime=`date "+%Y-%m-%d-%H:%M:%S"`

CGO_ENABLED=0 go build csiplugin-connector.go

if [ "$1" == "" ]; then
  docker build -t=registry.cn-hangzhou.aliyuncs.com/plugins/csi-ossglobal:$version-$GIT_SHA ./
  docker push registry.cn-hangzhou.aliyuncs.com/plugins/csi-ossglobal:$version-$GIT_SHA
fi
