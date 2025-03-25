#!/bin/bash
set -xe
branch=$1

cd ~/go/src/github.com/codingeasygo/util
git pull
util_sha=`git rev-parse HEAD`

cd ~/go/src/github.com/wfunc/crud
go get github.com/codingeasygo/util@$util_sha
go mod tidy
