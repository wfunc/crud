#!/bin/bash
set -xe
branch=$1

cd ~/go/src/github.com/wfunc/util
git pull
util_sha=`git rev-parse HEAD`

cd ~/go/src/github.com/wfunc/crud
go get github.com/wfunc/util@$util_sha
go mod tidy
