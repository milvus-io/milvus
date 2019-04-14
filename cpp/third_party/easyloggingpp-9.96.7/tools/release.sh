#!/bin/bash

# Bash script that helps with releasing new versions of Easylogging++
# Revision: 1.5
# author @abumusamq
#
# Usage:
#        ./release.sh [repo-root] [homepage-repo-root] [curr-version] [new-version] [do-not-ask]

if [ "$1" = "" ];then
  echo
  echo "Usage: $0 [repository-root] [homepage-root] [curr-version] [new-version] [do-not-ask]"
  echo
  exit 1
fi

if [ -f "$1/tools/release.sh" ];then
  [ -d "$2/releases/" ] || mkdir $2/releases/
else
  echo "Invalid repository root"
  exit 1
fi

CURR_VERSION=$3
CURR_RELEASE_DATE=$(grep -o '[0-9][0-9]-[0-9][0-9]-201[2-9] [0-9][0-9][0-9][0-9]hrs' $1/src/easylogging++.cc)
NEW_RELEASE_DATE=$(date +"%d-%m-%Y %H%Mhrs")
NEW_RELEASE_DATE_SIMPLE=$(date +"%d-%m-%Y")
NEW_VERSION=$4
DO_NOT_CONFIRM=$5
if [ "$NEW_VERSION" = "" ]; then
  echo 'Current Version  ' $CURR_VERSION
  echo '** No version provided **'
  exit
fi

echo 'Current Version  ' $CURR_VERSION ' (' $CURR_RELEASE_DATE ')'
echo 'New Version      ' $NEW_VERSION  ' (' $NEW_RELEASE_DATE ')'
if [ "$DO_NOT_CONFIRM" = "y" ]; then
  confirm="y"
else
  echo "Are you sure you wish to release new version [$CURR_VERSION -> $NEW_VERSION]? (y/n)"
  read confirm
fi

if [ "$confirm" = "y" ]; then
  sed -i '' -e "s/Easylogging++ v$CURR_VERSION*/Easylogging++ v$NEW_VERSION/g" $1/src/easylogging++.h
  sed -i '' -e "s/Easylogging++ v$CURR_VERSION*/Easylogging++ v$NEW_VERSION/g" $1/src/easylogging++.cc
  sed -i '' -e "s/Easylogging++ v$CURR_VERSION*/Easylogging++ v$NEW_VERSION/g" $1/README.md
  sed -i '' -e "s/return std::string(\"$CURR_VERSION\");/return std\:\:string(\"$NEW_VERSION\");/g" $1/src/easylogging++.cc
  sed -i '' -e "s/return std::string(\"$CURR_RELEASE_DATE\");/return std\:\:string(\"$NEW_RELEASE_DATE\");/g" $1/src/easylogging++.cc
  sed -i '' -e "s/\[Unreleased\]/\[$NEW_VERSION\] - $NEW_RELEASE_DATE_SIMPLE/g" $1/CHANGELOG.md
  astyle $1/src/easylogging++.h --style=google --indent=spaces=2 --max-code-length=120
  astyle $1/src/easylogging++.cc --style=google --indent=spaces=2 --max-code-length=120
  if [ -f "$1/src/easylogging++.h.orig" ];then
    rm $1/src/easylogging++.h.orig
  fi
  if [ -f "$1/src/easylogging++.cc.orig" ];then
    rm $1/src/easylogging++.cc.orig
  fi
  sed -i '' -e "s/$CURR_VERSION ($CURR_RELEASE_DATE)/$NEW_VERSION ($NEW_RELEASE_DATE)/g" $2/index.html
  sed -i '' -e "s/$CURR_VERSION/$NEW_VERSION/g" $1/README.md
  sed -i '' -e "s/easyloggingpp_$CURR_VERSION.zip/easyloggingpp_$NEW_VERSION.zip/g" $1/README.md
  if [ -f "easyloggingpp_v$NEW_VERSION.zip" ]; then
    rm easyloggingpp_v$NEW_VERSION.zip
  fi
  if [ -f "easyloggingpp.zip" ]; then
    rm easyloggingpp.zip
  fi
  cp $1/src/easylogging++.h .
  cp $1/src/easylogging++.cc .
  cp $1/CHANGELOG.md CHANGELOG.txt
  cp $1/README.md README.txt
  cp $1/LICENSE LICENSE.txt
  zip easyloggingpp_v$NEW_VERSION.zip easylogging++.h easylogging++.cc LICENSE.txt CHANGELOG.txt README.txt
  tar -pczf easyloggingpp_v$NEW_VERSION.tar.gz easylogging++.h easylogging++.cc LICENSE.txt CHANGELOG.txt README.txt
  mv easyloggingpp_v$NEW_VERSION.zip $2/
  mv easyloggingpp_v$NEW_VERSION.tar.gz $2/
  rm easylogging++.h easylogging++.cc CHANGELOG.txt LICENSE.txt README.txt
  echo "\n---------- PLEASE CHANGE CMakeLists.txt MANUALLY ----------- \n"
fi
