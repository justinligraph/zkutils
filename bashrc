#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pushd $DIR >/dev/null
export GOPATH=$(pwd)
popd >/dev/null

