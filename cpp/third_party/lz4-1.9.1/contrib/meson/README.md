Meson build system for lz4
==========================

Meson is a build system designed to optimize programmer productivity.
It aims to do this by providing simple, out-of-the-box support for
modern software development tools and practices, such as unit tests,
coverage reports, Valgrind, CCache and the like.

This Meson build system is provided with no guarantee.

## How to build

`cd` to this meson directory (`contrib/meson`)

```sh
meson setup --buildtype=release -Ddefault_library=shared -Dbuild_programs=true builddir
cd builddir
ninja             # to build
ninja install     # to install
```

You might want to install it in staging directory:

```sh
DESTDIR=./staging ninja install
```

To configure build options, use:

```sh
meson configure
```

See [man meson(1)](https://manpages.debian.org/testing/meson/meson.1.en.html).
