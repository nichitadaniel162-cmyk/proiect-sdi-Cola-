package ro.ubm.sd.common;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public final class TcpUtil {

    private TcpUtil() {}

    // Citeste exact o linie pana la '\n' fara a pre-incarca octeti din continutul binar al fisierului
    public static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') {
                break;
            }
            if (b != '\r') {
                buffer.write(b);
            }
        }
        if (b == -1 && buffer.size() == 0) {
            return null;
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    public static void writeLine(OutputStream out, String line) throws IOException {
        out.write((line + "\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    // Copiaza exact 'size' octeti si returneaza checksum-ul SHA-256 calculat pe parcurs
    public static String copyExactAndHash(InputStream in, OutputStream out, long size) throws IOException {
        MessageDigest digest = ChecksumUtil.newSha256();
        byte[] buffer = new byte[8192];
        long remaining = size;
        while (remaining > 0) {
            int toRead = (int) Math.min(buffer.length, remaining);
            int read = in.read(buffer, 0, toRead);
            if (read == -1) {
                throw new EOFException("Conexiune intrerupta prematur; mai lipseau " + remaining + " octeti");
            }
            out.write(buffer, 0, read);
            digest.update(buffer, 0, read);
            remaining -= read;
        }
        out.flush();
        return ChecksumUtil.bytesToHex(digest.digest());
    }
}
