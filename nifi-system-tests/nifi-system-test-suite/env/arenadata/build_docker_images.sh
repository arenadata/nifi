#!/bin/bash
git submodule update --init --recursive
IMAGE_VERSION=$1
MAVEN_PROFILE='test'

if [ -z "$IMAGE_VERSION" ]
then
  IMAGE_VERSION=it
fi

# Build ADB image
docker build -f ./Dockerfile-adb -t gpdb6-distributed:"${IMAGE_VERSION}" .
