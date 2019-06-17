#!/bin/bash

function kill_progress()
{
  kill -s SIGUSR2 $(pgrep $1)

  sleep 2
}

STATUS=$(kill_progress "vecwise_server" )

if [[ ${STATUS} == "false" ]];then
  echo "vecwise_server closed abnormally!"
else
  echo "vecwise_server closed successfully!"
fi
