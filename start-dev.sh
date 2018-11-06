#!/bin/bash

WORKDRIR=/go/src/github.com/teston/google-cloud-pubsub-helper

docker run \
    --rm \
    -it \
    --workdir ${WORKDRIR} \
    -v $(pwd):${WORKDRIR} \
    golang:1.11
    bash
