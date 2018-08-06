#!/bin/bash

docker run \
    --rm \
    -it \
    -v $(pwd):/go/src/github.com/teston/google-cloud-pubsub-helper \
    library/golang
    bash
