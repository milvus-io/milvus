package org.rocksdb.util;

public class Environment {
  private static String OS = System.getProperty("os.name").toLowerCase();
  private static String ARCH = System.getProperty("os.arch").toLowerCase();

  public static boolean isPowerPC() {
    return ARCH.contains("ppc");
  }

  public static boolean isS390x() {
    return ARCH.contains("s390x");
  }

  public static boolean isWindows() {
    return (OS.contains("win"));
  }

  public static boolean isFreeBSD() {
    return (OS.contains("freebsd"));
  }

  public static boolean isMac() {
    return (OS.contains("mac"));
  }

  public static boolean isAix() {
    return OS.contains("aix");
  }
  
  public static boolean isUnix() {
    return OS.contains("nix") ||
        OS.contains("nux");
  }

  public static boolean isSolaris() {
    return OS.contains("sunos");
  }

  public static boolean isOpenBSD() {
    return (OS.contains("openbsd"));
  }

  public static boolean is64Bit() {
    if (ARCH.indexOf("sparcv9") >= 0) {
      return true;
    }
    return (ARCH.indexOf("64") > 0);
  }

  public static String getSharedLibraryName(final String name) {
    return name + "jni";
  }

  public static String getSharedLibraryFileName(final String name) {
    return appendLibOsSuffix("lib" + getSharedLibraryName(name), true);
  }

  public static String getJniLibraryName(final String name) {
    if (isUnix()) {
      final String arch = is64Bit() ? "64" : "32";
      if(isPowerPC()) {
        return String.format("%sjni-linux-%s", name, ARCH);
      } else if(isS390x()) {
        return String.format("%sjni-linux%s", name, ARCH);
      } else {
        return String.format("%sjni-linux%s", name, arch);
      }
    } else if (isMac()) {
      return String.format("%sjni-osx", name);
    } else if (isFreeBSD()) {
      return String.format("%sjni-freebsd%s", name, is64Bit() ? "64" : "32");
    } else if (isAix() && is64Bit()) {
      return String.format("%sjni-aix64", name);
    } else if (isSolaris()) {
      final String arch = is64Bit() ? "64" : "32";
      return String.format("%sjni-solaris%s", name, arch);
    } else if (isWindows() && is64Bit()) {
      return String.format("%sjni-win64", name);
    } else if (isOpenBSD()) {
      return String.format("%sjni-openbsd%s", name, is64Bit() ? "64" : "32");
    }

    throw new UnsupportedOperationException(String.format("Cannot determine JNI library name for ARCH='%s' OS='%s' name='%s'", ARCH, OS, name));
  }

  public static String getJniLibraryFileName(final String name) {
    return appendLibOsSuffix("lib" + getJniLibraryName(name), false);
  }

  private static String appendLibOsSuffix(final String libraryFileName, final boolean shared) {
    if (isUnix() || isAix() || isSolaris() || isFreeBSD() || isOpenBSD()) {
      return libraryFileName + ".so";
    } else if (isMac()) {
      return libraryFileName + (shared ? ".dylib" : ".jnilib");
    } else if (isWindows()) {
      return libraryFileName + ".dll";
    }
    throw new UnsupportedOperationException();
  }

  public static String getJniLibraryExtension() {
    if (isWindows()) {
      return ".dll";
    }
    return (isMac()) ? ".jnilib" : ".so";
  }
}
