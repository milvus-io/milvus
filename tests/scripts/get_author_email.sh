#!/bin/bash

set -e
function get_author_email(){
    email=$(git --no-pager show -s --format=\'%ae\' HEAD )
    if [[ "${email}" == 'nobody@nowhere' ]]; then 
        email=$(git --no-pager show -s --format=\'%ae\' HEAD^ )
    fi 
   echo ${email}
}
get_author_email