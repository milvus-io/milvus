
# Google Test #

[![Build Status](https://travis-ci.org/google/googletest.svg?branch=master)](https://travis-ci.org/google/googletest)
[![Build status](https://ci.appveyor.com/api/projects/status/4o38plt0xbo1ubc8/branch/master?svg=true)](https://ci.appveyor.com/project/BillyDonahue/googletest/branch/master)

Welcome to **Google Test**, Google's C++ test framework!

This repository is a merger of the formerly separate GoogleTest and
GoogleMock projects. These were so closely related that it makes sense to
maintain and release them together.

Please see the project page above for more information as well as the
mailing list for questions, discussions, and development.  There is
also an IRC channel on OFTC (irc.oftc.net) #gtest available.  Please
join us!

Getting started information for **Google Test** is available in the 
[Google Test Primer](googletest/docs/Primer.md) documentation.

**Google Mock** is an extension to Google Test for writing and using C++ mock
classes.  See the separate [Google Mock documentation](googlemock/README.md).

More detailed documentation for googletest (including build instructions) are
in its interior [googletest/README.md](googletest/README.md) file.

## Features ##

  * An [XUnit](https://en.wikipedia.org/wiki/XUnit) test framework.
  * Test discovery.
  * A rich set of assertions.
  * User-defined assertions.
  * Death tests.
  * Fatal and non-fatal failures.
  * Value-parameterized tests.
  * Type-parameterized tests.
  * Various options for running the tests.
  * XML test report generation.

## Platforms ##

Google test has been used on a variety of platforms:

  * Linux
  * Mac OS X
  * Windows
  * Cygwin
  * MinGW
  * Windows Mobile
  * Symbian

## Who Is Using Google Test? ##

In addition to many internal projects at Google, Google Test is also used by
the following notable projects:

  * The [Chromium projects](http://www.chromium.org/) (behind the Chrome
    browser and Chrome OS).
  * The [LLVM](http://llvm.org/) compiler.
  * [Protocol Buffers](https://github.com/google/protobuf), Google's data
    interchange format.
  * The [OpenCV](http://opencv.org/) computer vision library.

## Related Open Source Projects ##

[Google Test UI](https://github.com/ospector/gtest-gbar) is test runner that runs
your test binary, allows you to track its progress via a progress bar, and
displays a list of test failures. Clicking on one shows failure text. Google
Test UI is written in C#.

[GTest TAP Listener](https://github.com/kinow/gtest-tap-listener) is an event
listener for Google Test that implements the
[TAP protocol](https://en.wikipedia.org/wiki/Test_Anything_Protocol) for test
result output. If your test runner understands TAP, you may find it useful.

## Requirements ##

Google Test is designed to have fairly minimal requirements to build
and use with your projects, but there are some.  Currently, we support
Linux, Windows, Mac OS X, and Cygwin.  We will also make our best
effort to support other platforms (e.g. Solaris, AIX, and z/OS).
However, since core members of the Google Test project have no access
to these platforms, Google Test may have outstanding issues there.  If
you notice any problems on your platform, please notify
<googletestframework@googlegroups.com>. Patches for fixing them are
even more welcome!

### Linux Requirements ###

These are the base requirements to build and use Google Test from a source
package (as described below):

  * GNU-compatible Make or gmake
  * POSIX-standard shell
  * POSIX(-2) Regular Expressions (regex.h)
  * A C++98-standard-compliant compiler

### Windows Requirements ###

  * Microsoft Visual C++ v7.1 or newer

### Cygwin Requirements ###

  * Cygwin v1.5.25-14 or newer

### Mac OS X Requirements ###

  * Mac OS X v10.4 Tiger or newer
  * Xcode Developer Tools

### Requirements for Contributors ###

We welcome patches.  If you plan to contribute a patch, you need to
build Google Test and its own tests from a git checkout (described
below), which has further requirements:

  * [Python](https://www.python.org/) v2.3 or newer (for running some of
    the tests and re-generating certain source files from templates)
  * [CMake](https://cmake.org/) v2.6.4 or newer

## Regenerating Source Files ##

Some of Google Test's source files are generated from templates (not
in the C++ sense) using a script.
For example, the
file include/gtest/internal/gtest-type-util.h.pump is used to generate
gtest-type-util.h in the same directory.

You don't need to worry about regenerating the source files
unless you need to modify them.  You would then modify the
corresponding `.pump` files and run the '[pump.py](googletest/scripts/pump.py)'
generator script.  See the [Pump Manual](googletest/docs/PumpManual.md).

### Contributing Code ###

We welcome patches.  Please read the
[Developer's Guide](googletest/docs/DevGuide.md)
for how you can contribute. In particular, make sure you have signed
the Contributor License Agreement, or we won't be able to accept the
patch.

Happy testing!
