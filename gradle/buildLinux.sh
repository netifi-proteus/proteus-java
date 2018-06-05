#!/usr/bin/env bash
docker run  -e TRAVIS_PULL_REQUEST=$TRAVIS_PULL_REQUEST \
-e TRAVIS_TAG=$TRAVIS_TAG -e TRAVIS_BRANCH=$TRAVIS_BRANCH \
-e bintrayUser=$bintrayUser -e bintrayKey=$bintrayKey  \
-v `pwd`:/build netifi/proteus-java-build sh -c "gradle/buildViaTravis-linux.sh"
