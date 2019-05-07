#!/bin/sh
set -e

# Constants
SED_COMMANDS="commands.tmp"
CLANG_FORMAT="clang-format-3.9"
INCLUDE='include/linux/'
LIB='lib/zstd/'
SPACES='    '
TAB=$'\t'
TMP="replacements.tmp"

function prompt() {
  while true; do
    read -p "$1 [Y/n]" yn
    case $yn in
        '' ) yes='yes'; break;;
        [Yy]* ) yes='yes'; break;;
        [Nn]* ) yes=''; break;;
        * ) echo "Please answer yes or no.";;
    esac
done
}

function check_not_present() {
  grep "$1" $INCLUDE*.h ${LIB}*.{h,c} && exit 1 || true
}

function check_not_present_in_file() {
  grep "$1" "$2" && exit 1 || true
}

function check_present_in_file() {
  grep "$1" "$2" > /dev/null 2> /dev/null || exit 1
}

echo "Files: " $INCLUDE*.h $LIB*.{h,c}

prompt "Do you wish to replace 4 spaces with a tab?"
if [ ! -z "$yes" ]
then
  # Check files for existing tabs
  grep "$TAB" $INCLUDE*.h $LIB*.{h,c} && exit 1 || true
  # Replace the first tab on every line
  sed -i '' "s/^$SPACES/$TAB/" $INCLUDE*.h $LIB*.{h,c}

  # Execute once and then execute as long as replacements are happening
  more_work="yes"
  while [ ! -z "$more_work" ]
  do
    rm -f $TMP
    # Replaces $SPACES that directly follow a $TAB with a $TAB.
    # $TMP will be non-empty if any replacements took place.
    sed -i '' "s/$TAB$SPACES/$TAB$TAB/w $TMP" $INCLUDE*.h $LIB*.{h,c}
    more_work=$(cat "$TMP")
  done
  rm -f $TMP
fi

prompt "Do you wish to replace '{   ' with a tab?"
if [ ! -z "$yes" ]
then
  sed -i '' "s/$TAB{   /$TAB{$TAB/g" $INCLUDE*.h $LIB*.{h,c}
fi

rm -f $SED_COMMANDS
cat > $SED_COMMANDS <<EOF
s/current/curr/g
s/MEM_STATIC/ZSTD_STATIC/g
s/MEM_check/ZSTD_check/g
s/MEM_32bits/ZSTD_32bits/g
s/MEM_64bits/ZSTD_64bits/g
s/MEM_LITTLE_ENDIAN/ZSTD_LITTLE_ENDIAN/g
s/MEM_isLittleEndian/ZSTD_isLittleEndian/g
s/MEM_read/ZSTD_read/g
s/MEM_write/ZSTD_write/g
EOF

prompt "Do you wish to run these sed commands $(cat $SED_COMMANDS)?"
if [ ! -z "$yes" ]
then
  sed -i '' -f $SED_COMMANDS $LIB*.{h,c}
fi
rm -f $SED_COMMANDS

prompt "Do you wish to clang-format $LIB*.{h,c}?"
if [ ! -z "$yes" ]
then
  $CLANG_FORMAT -i ${LIB}*.{h,c}
fi

prompt "Do you wish to run some checks?"
if [ ! -z "$yes" ]
then
  check_present_in_file ZSTD_STATIC_ASSERT ${LIB}zstd_internal.h
  check_not_present_in_file STATIC_ASSERT ${LIB}mem.h
  check_not_present_in_file "#define ZSTD_STATIC_ASSERT" ${LIB}compress.c
  check_not_present MEM_STATIC
  check_not_present FSE_COMMONDEFS_ONLY
  check_not_present "#if 0"
  check_not_present "#if 1"
  check_not_present _MSC_VER
  check_not_present __cplusplus
  check_not_present __STDC_VERSION__
  check_not_present __VMS
  check_not_present __GNUC__
  check_not_present __INTEL_COMPILER
  check_not_present FORCE_MEMORY_ACCESS
  check_not_present STATIC_LINKING_ONLY
fi
