#!/usr/bin/env bash

# Reference: https://github.com/ryandaniels/docker-script-find-latest-image-tag/blob/master/docker_image_find_tag.sh

set -ef -o pipefail

REGISTRY=${REGISTRY:-"https://index.docker.io/v2"}
REGISTRY_AUTH=${REGISTRY_AUTH:-"https://auth.docker.io"}
REGISTRY_SERVICE=${REGISTRY_SERVICE:-"registry.docker.io"}   
# IMAGE_NAME=library/traefik
IMAGE_NAME=${IMAGE_NAME:-""}
IMAGE_TAG=${IMAGE_TAG-""}
IMAGE_ID_SHORT=${IMAGE_ID_SHORT:-""}
# IMAGE_ID_SHORT="96c63a7d3e50"
IMAGE_ID_TARGET=""
IMAGE_ID_LONG=${IMAGE_ID_LONG:-""}
# IMAGE_ID_LONG="sha256:96c63a7d3e502fcbbd8937a2523368c22d0edd1788b8389af095f64038318834"
DOCKER_BIN=docker
# TAGS_FILTER="1.7"
TAGS_FILTER=${TAGS_FILTER:-""}
FAST_MATCH=0
VERBOSE=0
QUIET=0
TAGS_LIMIT=100
TAG_UNLIMIT=0
ignore_404=0

show_help () {
  echo "Usage:"
  echo "$0 [-n image name] [-i image-id]"
  echo "Example: $0 -n traefik -i 96c63a7d3e50 -f 1.7"
  echo "  -n [text]: Image name (Required). '-n traefik' would reference the traefik image"
  echo "  -D: Use Docker binary for Image ID check (Default) (Optional)"
  echo "  -P: Use Podman binary for Image ID check (Optional)"
  echo "  -r [text]: Registry URL to use. Example: -r https://index.docker.io/v2 (Default) (Optional)"
  echo "  -a [text]: Registry AUTH to use. Example: -a https://auth.docker.io (Default) (Optional)"  
  echo "  -l [number]: Tag limit. Defaults to 100. (Optional)"  
  echo "  -L: Tag unlimit. (Optional)"
  echo "  -f [text]: Filter tag to contain this value (Optional)"
  echo "  -F: Fast match tag. (Optional)"
  echo "  -q: Quiet output (Optional)"
  echo "  -v: Verbose output (Optional)"
}

# From: https://stackoverflow.com/questions/192249/how-do-i-parse-command-line-arguments-in-bash
# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.

while getopts "h?n:t:DPr:a:l:Lf:Fqv" opt; do
    case "$opt" in
    h|\?)
        show_help
        exit 0
        ;;
    n)  IMAGE_NAME="$OPTARG"
        ;;
    t)  IMAGE_TAG="$OPTARG"
        ;;
    D)  DOCKER_BIN=docker
        ;;
    P)  DOCKER_BIN=podman
        ;;
    r)  REGISTRY="$OPTARG"
        ;;
    a)  REGISTRY_AUTH="$OPTARG"
        ;;
    l)  TAGS_LIMIT="$OPTARG"
        ;;
    L)  TAG_UNLIMIT=1
        ;;
    f)  TAGS_FILTER="$OPTARG"
        ;;
    F)  FAST_MATCH=1
        ;;
    q)  QUIET=1
        ;;
    v)  VERBOSE=1
        ;;
    esac
done

shift $((OPTIND-1))

[ "${1:-}" = "--" ] && shift

if [ -z "$IMAGE_NAME" ]; then
  echo "Requires Image Name"
  exit 1;
else
  if [[ "$VERBOSE" -eq 1 ]]; then
    echo "Using IMAGE_NAME: $IMAGE_NAME"
  fi
  # add library/ if no /. (Which is _ aka official image like hub.docker.com/_/traefik)
  # Official images are in "library/"
  if [[ "$IMAGE_NAME" != *"/"* ]]; then
      IMAGE_NAME="library/$IMAGE_NAME"
  fi
fi

if [[ -z "$IMAGE_TAG" ]]; then
  echo "Requires Image Tag"
  exit 1;
fi

if [[ "$VERBOSE" -eq 1 ]]; then
  echo "Using REGISTRY: $REGISTRY"
fi

if ! [[ $TAGS_LIMIT =~ ^[0-9]+$ ]] ; then
  echo "Tag limit (-l) must be an integer > 0"
  exit 1;
fi

function log() {
  if [[ "${QUIET}" -ne 1 || "$VERBOSE" -eq 1  ]]; then
    echo "${1}"
  fi
}

# https://unix.stackexchange.com/questions/459367/using-shell-variables-for-command-options/459369#459369
# https://unix.stackexchange.com/questions/444946/how-can-we-run-a-command-stored-in-a-variable/444949#444949
# https://askubuntu.com/questions/674333/how-to-pass-an-array-as-function-argument/995110#995110
# Maybe this? https://stackoverflow.com/questions/45948172/executing-a-curl-request-through-bash-script/45948289#45948289
# http://mywiki.wooledge.org/BashFAQ/050#I_only_want_to_pass_options_if_the_runtime_data_needs_them
function do_curl_get () {
  local URL="$1"
  shift
  local array=("$@")
  HTTP_RESPONSE="$(curl -sSL --write-out "HTTPSTATUS:%{http_code}" \
    -H "Content-Type: application/json;charset=UTF-8" \
    "${array[@]}" \
    -X GET "$URL")"
  HTTP_BODY=$(echo "$HTTP_RESPONSE" | sed -E 's/HTTPSTATUS\:[0-9]{3}$//')
  HTTP_STATUS=$(echo "$HTTP_RESPONSE" | tr -d '\n' | sed -E 's/.*HTTPSTATUS:([0-9]{3})$/\1/')
  # Check that the http status is 200
  if [[ "$HTTP_STATUS" -ne 200 ]]; then
    if [[ "$ignore_404" -eq 0 ]]; then
      if [[ "$VERBOSE" -eq 0 ]]; then
        echo -e "\\nError $HTTP_STATUS from: $URL\\n"
      else
        echo -e "\\nError $HTTP_STATUS from: $URL\\nHTTP_BODY: $HTTP_BODY\\n"
      fi
      exit 1
    fi
  fi
}

# Get AUTH token
CURL_AUTH=()
CURL_URL="$REGISTRY_AUTH/token?service=${REGISTRY_SERVICE##*(//)}&scope=repository:$IMAGE_NAME:pull"
do_curl_get "$CURL_URL" "${CURL_AUTH[@]}"
AUTH=$(echo "$HTTP_BODY" | jq --raw-output .token)

# Get Source Image ID
CURL_AUTH=( -H "Authorization: Bearer $AUTH" -H "Accept:application/vnd.docker.distribution.manifest.v2+json" )
CURL_URL="$REGISTRY/$IMAGE_NAME/manifests/$IMAGE_TAG"
do_curl_get "$CURL_URL" "${CURL_AUTH[@]}"
IMAGE_ID_SOURCE="$(echo "$HTTP_BODY" |jq -r .config.digest)"

# Get Tags
CURL_AUTH=( -H "Authorization: Bearer $AUTH" )
CURL_URL="$REGISTRY/$IMAGE_NAME/tags/list"
do_curl_get "$CURL_URL" "${CURL_AUTH[@]}"
TAGS_CURL=$(echo "$HTTP_BODY")
TAGS_COUNT=$(echo "$TAGS_CURL"|jq -r '.tags[]'|grep -vi windows|wc -l)
TAGS_temp=$(echo "$TAGS_CURL"|jq --arg TAGS_FILTER "$TAGS_FILTER" -r '.tags[]|select(.|contains($TAGS_FILTER))'|grep -vi windows|sort -r --version-sort)

if [[ "$TAG_UNLIMIT" -eq 1 ]]; then
  TAGS_LIMIT="$TAGS_COUNT"
fi

TAGS=$(echo "$TAGS_temp"|sed -n 1,"$TAGS_LIMIT"p)
log "Found Total Tags: $TAGS_COUNT"
# Check if tags are not being filtered
if [ -z "$TAGS_FILTER" ]; then
  log "Limiting Tags to: $TAGS_LIMIT"
  # Check if limit reached and display warning
  if [[ "$TAGS_COUNT" -gt "$TAGS_LIMIT" ]]; then
    log "Limit reached, consider increasing limit (-l [number]) or adding a filter (-f [text])"
  fi
# If tags are filtered, show how many filtered tags were found
else
  TAGS_FILTER_COUNT=$(echo "$TAGS_temp"|wc -l)
  log "Found Tags (after filtering): $TAGS_FILTER_COUNT"
  log "Limiting Tags to: $TAGS_LIMIT"
  # Check if limit reached and display warning
  if [[ "$TAGS_FILTER_COUNT" -ge "$TAGS_LIMIT" ]]; then
    log "Limit reached, consider increasing limit (-l [number]) or use more specific filter (-f [text])"
  fi
fi
if [[ "$VERBOSE" -eq 1 ]]; then
  # Output all tags found
  echo -e "\nFound Tags:\n$TAGS"
fi

# Loop through tags and look for sha Id match
# Some "manifests/tag" endpoints do not exist (http404 error)? Seems to be windows images. Ignore any 404 error
ignore_404=1
counter=0
log "Checking for image match ..."

if [[ "$FAST_MATCH" -eq 1 ]]; then
    log "Fast match tag."
fi
for i in $TAGS; do
  if [[ "$VERBOSE" -eq 1 ]]; then
  # Output still working text every 50 tags if -v
    if [[ "$counter" =~ ^($(echo {50..1000..50}|sed 's/ /|/g'))$ ]]; then
      log "Still working, currently on tag number: $counter"
    fi
    counter=$((counter+1))
  fi

  if [[ "$IMAGE_TAG" != "$i" ]]; then
    # IMAGE_ID_TARGET="$(curl -sSLH "Authorization: Bearer $AUTH" -H "Accept:application/vnd.docker.distribution.manifest.v2+json" -X GET "$REGISTRY/$IMAGE_NAME/manifests/$i"|jq -r .config.digest)"
    CURL_AUTH=( -H "Authorization: Bearer $AUTH" -H "Accept:application/vnd.docker.distribution.manifest.v2+json" )
    CURL_URL="$REGISTRY/$IMAGE_NAME/manifests/$i"
    do_curl_get "$CURL_URL" "${CURL_AUTH[@]}"
    IMAGE_ID_TARGET="$(echo "$HTTP_BODY" |jq -r .config.digest)"

    if [[ "$IMAGE_ID_TARGET" == "$IMAGE_ID_SOURCE" ]]; then
        log "Found match. tag:"
        echo "$i"

        if [[ "$FAST_MATCH" -eq 1 ]]; then
          exit 0
        fi
    fi
  fi
done;
