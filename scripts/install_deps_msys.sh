#!/bin/sh

set -e

if [[ "${MSYSTEM}" != "MINGW64" ]] ; then
    echo non MINGW64, exit.
    exit 1
fi

pacmanInstall()
{
  pacman -S --noconfirm --needed \
    git make tar dos2unix zip unzip patch \
    mingw-w64-x86_64-toolchain \
    mingw-w64-x86_64-make \
    mingw-w64-x86_64-ccache \
    mingw-w64-x86_64-cmake \
    mingw-w64-x86_64-openblas \
    mingw-w64-x86_64-clang \
    mingw-w64-x86_64-clang-tools-extra \
    mingw-w64-x86_64-python2 \
    mingw-w64-x86_64-python-pip \
    mingw-w64-x86_64-diffutils \
    mingw-w64-x86_64-go

  pip3 install conan==1.61.0
}

updateKey()
{
    pacman-key --refresh-keys
}

pacmanInstall || {
    updateKey
    pacmanInstall

}


# dummy empty dl, TODO: remove later
touch a.c && \
    gcc -c a.c && \
    ar rc libdl.a a.o && \
    ranlib libdl.a && \
    cp -fr libdl.a /mingw64/lib && \
    rm -fr a.c a.o libdl.a

