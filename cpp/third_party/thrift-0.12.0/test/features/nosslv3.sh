#!/bin/bash

#
# Checks to make sure SSLv3 is not allowed by a server.
#

THRIFTHOST=localhost
THRIFTPORT=9090

while [[ $# -ge 1 ]]; do
  arg="$1"
  argIN=(${arg//=/ })

  case ${argIN[0]} in
    -h|--host)
    THRIFTHOST=${argIN[1]}
    shift # past argument
    ;;
    -p|--port)
    THRIFTPORT=${argIN[1]}
    shift # past argument
    ;;
    *)
          # unknown option ignored
    ;;
  esac

  shift   # past argument or value
done

function nosslv3
{
  local nego
  local negodenied
  local opensslv

  opensslv=$(openssl version | cut -d' ' -f2)
  if [[ $opensslv > "1.0" ]]; then
    echo "[pass] OpenSSL 1.1 or later - no need to check ssl3"
    return 0
  fi

  # echo "openssl s_client -connect $THRIFTHOST:$THRIFTPORT -CAfile ../keys/CA.pem -ssl3 2>&1 < /dev/null"
  nego=$(openssl s_client -connect $THRIFTHOST:$THRIFTPORT -CAfile ../keys/CA.pem -ssl3 2>&1 < /dev/null)
  negodenied=$?

  if [[ $negodenied -ne 0 ]]; then
    echo "[pass] SSLv3 negotiation disabled"
    echo $nego
    return 0
  fi

  echo "[fail] SSLv3 negotiation enabled!  stdout:"
  echo $nego
  return 1
}

nosslv3
exit $?
