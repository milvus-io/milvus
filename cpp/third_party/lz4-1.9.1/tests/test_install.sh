#/usr/bin/env sh
set -e

make="make -C $lz4_root"
for cmd in install uninstall; do
  for upper in DUMMY PREFIX EXEC_PREFIX LIBDIR INCLUDEDIR PKGCONFIGDIR BINDIR MANDIR MAN1DIR ; do
    lower=$(echo $upper | tr '[:upper:]' '[:lower:]')
    tmp_lower="$(pwd)/tmp-lower-$lower/"
    tmp_upper="$(pwd)/tmp-upper-$lower/"
    echo $make $cmd DESTDIR="$tmp_upper" $upper="test"
    $make $cmd DESTDIR="$tmp_upper" $upper="test" >/dev/null
    echo $make $cmd DESTDIR="$tmp_lower" $lower="test"
    $make $cmd DESTDIR="$tmp_lower" $lower="test" >/dev/null
    command diff -r "$tmp_lower" "$tmp_upper" && echo "SAME!" || false
    if [ "x$cmd" = "xuninstall" ]; then
      test -z "$(find "$tmp_lower" -type f)" && echo "EMPTY!" || false
      rm -rf "$tmp_upper" "$tmp_lower"
    fi
  done
done
