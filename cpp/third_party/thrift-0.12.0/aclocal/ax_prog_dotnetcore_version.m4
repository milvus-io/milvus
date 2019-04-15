# ===============================================================================
#  https://www.gnu.org/software/autoconf-archive/ax_prog_dotnetcore_version.html
# ===============================================================================
#
# SYNOPSIS
#
#   AX_PROG_DOTNETCORE_VERSION([VERSION],[ACTION-IF-TRUE],[ACTION-IF-FALSE])
#
# DESCRIPTION
#
#   Makes sure that .NET Core supports the version indicated. If true the
#   shell commands in ACTION-IF-TRUE are executed. If not the shell commands
#   in ACTION-IF-FALSE are run. The $dotnetcore_version variable will be
#   filled with the detected version.
#
#   This macro uses the $DOTNETCORE variable to perform the check. If
#   $DOTNETCORE is not set prior to calling this macro, the macro will fail.
#
#   Example:
#
#     AC_PATH_PROG([DOTNETCORE],[dotnet])
#     AC_PROG_DOTNETCORE_VERSION([1.0.2],[ ... ],[ ... ])
#
#   Searches for .NET Core, then checks if at least version 1.0.2 is
#   present.
#
# LICENSE
#
#   Copyright (c) 2016 Jens Geyer <jensg@apache.org>
#
#   Copying and distribution of this file, with or without modification, are
#   permitted in any medium without royalty provided the copyright notice
#   and this notice are preserved. This file is offered as-is, without any
#   warranty.

#serial 2

AC_DEFUN([AX_PROG_DOTNETCORE_VERSION],[
    AC_REQUIRE([AC_PROG_SED])

    AS_IF([test -n "$DOTNETCORE"],[
        ax_dotnetcore_version="$1"

        AC_MSG_CHECKING([for .NET Core version])
        dotnetcore_version=`$DOTNETCORE --version 2>&1 | $SED -e 's/\(@<:@0-9@:>@*\.@<:@0-9@:>@*\.@<:@0-9@:>@*\)\(.*\)/\1/'`
        AC_MSG_RESULT($dotnetcore_version)

	    AC_SUBST([DOTNETCORE_VERSION],[$dotnetcore_version])

        AX_COMPARE_VERSION([$ax_dotnetcore_version],[le],[$dotnetcore_version],[
	    :
            $2
        ],[
	    :
            $3
        ])
    ],[
        AC_MSG_WARN([could not find .NET Core])
        $3
    ])
])
