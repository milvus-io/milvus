#!/bin/bash

#
# Checks to make sure TLSv1.0 or later is allowed by a server.
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

declare -A EXPECT_NEGOTIATE
EXPECT_NEGOTIATE[tls1]=1
EXPECT_NEGOTIATE[tls1_1]=1
EXPECT_NEGOTIATE[tls1_2]=1

failures=0

function tls
{
  for PROTO in "${!EXPECT_NEGOTIATE[@]}"; do

    local nego
    local negodenied
    local res

    echo "openssl s_client -connect $THRIFTHOST:$THRIFTPORT -CAfile ../keys/CA.pem -$PROTO 2>&1 < /dev/null"
    nego=$(openssl s_client -connect $THRIFTHOST:$THRIFTPORT -CAfile ../keys/CA.pem -$PROTO 2>&1 < /dev/null)
    negodenied=$?
    echo "result of command: $negodenied"

    res="enabled"; if [[ ${EXPECT_NEGOTIATE[$PROTO]} -eq 0 ]]; then res="disabled"; fi

    if [[ $negodenied -ne ${EXPECT_NEGOTIATE[$PROTO]} ]]; then
      echo "$PROTO negotiation allowed"
    else
      echo "[warn] $PROTO negotiation did not work"
      echo $nego
      ((failures++))
    fi
  done
}

tls

if [[ $failures -eq 3 ]]; then
  echo "[fail] At least one of TLSv1.0, TLSv1.1, or TLSv1.2 needs to work, but does not"
  exit $failures
fi

echo "[pass] At least one of TLSv1.0, TLSv1.1, or TLSv1.2 worked"
exit 0
