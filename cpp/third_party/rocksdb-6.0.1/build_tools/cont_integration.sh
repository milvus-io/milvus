#!/usr/bin/env bash
#
# Copyright (c) 2016, Facebook. All rights reserved.
#
# Overall wrapper script for RocksDB continuous builds. The implementation is a
# trivial pulling scheme. We loop infinitely, check if any new changes have been
# committed, if yes then trigger a Sandcastle run, and finally go to sleep again
# for a certain interval.
#

SRC_GIT_REPO=/data/git/rocksdb-public
error=0

function log {
  DATE=`date +%Y-%m-%d:%H:%M:%S`
  # shellcheck disable=SC2068
  echo $DATE $@
}

function log_err {
  # shellcheck disable=SC2145
  log "ERROR: $@ Error code: $error."
}

function update_repo_status {
  # Update the parent first.
  pushd $SRC_GIT_REPO

  # This is a fatal error. Something in the environment isn't right and we will
  # terminate the execution.
  error=$?
  if [ ! $error -eq 0 ]; then
    log_err "Where is $SRC_GIT_REPO?"
    exit $error
  fi

  HTTPS_PROXY=fwdproxy:8080 git fetch -f

  error=$?
  if [ ! $error -eq 0 ]; then
    log_err "git fetch -f failed."
    popd
    return $error
  fi

  git update-ref refs/heads/master refs/remotes/origin/master

  error=$?
  if [ ! $error -eq 0 ]; then
    log_err "git update-ref failed."
    popd
    return $error
  fi

  popd

  # We're back in an instance-specific directory. Get the latest changes.
  git pull --rebase

  error=$?
  if [ ! $error -eq 0 ]; then
    log_err "git pull --rebase failed."
    return $error
  fi
}

#
# Execution starts here.
#

# Path to the determinator from the root of the RocksDB repo.
CONTRUN_DETERMINATOR=./build_tools/RocksDBCommonHelper.php

# Value of the previous commit.
PREV_COMMIT=

log "Starting to monitor for new RocksDB changes ..."
log "Running under `pwd` as `whoami`."

# Paranoia. Make sure that we're using the right branch.
git checkout master

error=$?
if [ ! $error -eq 0 ]; then
  log_err "This is not good. Can't checkout master. Bye-bye!"
  exit 1
fi

# We'll run forever and let the execution environment terminate us if we'll
# exceed whatever timeout is set for the job.
while true;
do
  # Get the latest changes committed.
  update_repo_status

  error=$?
  if [  $error -eq 0 ]; then
    LAST_COMMIT=`git log -1 | head -1 | grep commit | awk '{ print $2; }'`

    log "Last commit is '$LAST_COMMIT', previous commit is '$PREV_COMMIT'."

    if [ "$PREV_COMMIT" == "$LAST_COMMIT" ]; then
      log "There were no changes since the last time I checked. Going to sleep."
    else
      if [ ! -z "$LAST_COMMIT" ]; then
        log "New code has been committed or previous commit not known. " \
            "Will trigger the tests."

        PREV_COMMIT=$LAST_COMMIT
        log "Updated previous commit to '$PREV_COMMIT'."

        #
        # This is where we'll trigger the Sandcastle run. The values for
        # HTTPS_APP_VALUE and HTTPS_APP_VALUE will be set in the container we're
        # running in.
        #
        POST_RECEIVE_HOOK=1 php $CONTRUN_DETERMINATOR

        error=$?
        if [ $error -eq 0 ]; then
          log "Sandcastle run successfully triggered."
        else
          log_err "Failed to trigger Sandcastle run."
        fi
      else
        log_err "Previous commit not updated. Don't know what the last one is."
      fi
    fi
  else
    log_err "Getting latest changes failed. Will skip running tests for now."
  fi

  # Always sleep, even if errors happens while trying to determine the latest
  # commit. This will prevent us terminating in case of transient errors.
  log "Will go to sleep for 5 minutes."
  sleep 5m
done
