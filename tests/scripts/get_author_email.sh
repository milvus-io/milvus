#!/bin/bash
# author emaill will be jenkins's email, when the pr branch is not fast forward compared with master branch
# Exit immediately for non zero status
set -e
function get_author_email(){
    email=$(git --no-pager show -s --format=\'%ae\' HEAD )
    if [[ "${email}" == \'nobody@nowhere\' ]]; then 
        email=$(git --no-pager show -s --format=\'%ae\' HEAD^ )
    fi 
   echo ${email} | sed $'s/\'//g'
}
get_author_email