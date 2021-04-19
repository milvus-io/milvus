dnl
dnl Check for an nm(1) utility.
dnl
AC_DEFUN([FA_PROG_NM],
[
    case "${NM-unset}" in
        unset) AC_CHECK_PROGS(NM, nm, nm) ;;
        *) AC_CHECK_PROGS(NM, $NM nm, nm) ;;
    esac
    AC_MSG_CHECKING(nm flags)
    case "${NMFLAGS-unset}" in
        unset) NMFLAGS= ;;
    esac
    AC_MSG_RESULT($NMFLAGS)
    AC_SUBST(NMFLAGS)
])
