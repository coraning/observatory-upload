package ch.zhaw.mami;

public class Util {

    public static String byteArr2HexStr(final byte[] b) {
        String result = "";
        for (int i = 0; i < b.length; i++) {
            result += Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1);
        }
        return result;
    }

    public static boolean validateFileName(final String string) {
        /*
         * if (string.indexOf('.') != string.lastIndexOf('.')) { return false; }
         */

        if (!string.matches("[a-zA-Z0-9\\.\\-]*")) {
            return false;
        }

        /*
         * if (string.contains("..")) { return false; }
         */

        if (string.length() > 64) {
            return false;
        }

        return true;
    }

    public static boolean validatePath(final String string) {
        if (!string.matches("[a-zA-Z0-9\\-/\\.]*")) {
            return false;
        }

        if (string.contains("..")) {
            return false;
        }

        return true;
    }

    public static boolean validatePathPart(final String string) {
        if (!string.matches("[a-zA-Z0-9\\-]*")) {
            return false;
        }

        if (string.length() > 32) {
            return false;
        }

        return true;
    }
}
