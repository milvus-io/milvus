#!/bin/bash

function kill_progress()
{
  kill -s SIGUSR2 $(pgrep $1)

  sleep 2
}

STATUS=$(kill_progress "milvus_server" )

if [[ ${STATUS} == "false" ]];then
  echo "Milvus server closed abnormally!"
else
  echo "Milvus server closed successfully!"
fi
