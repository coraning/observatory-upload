package ch.zhaw.mami.validation;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.Arrays;

import ch.zhaw.mami.RuntimeConfiguration;

public class Validator {

    private static void doValidate(final ProcessBuilder pb)
            throws InterruptedException, IOException, ValidationException {
        pb.redirectOutput(Redirect.PIPE);
        Process proc = pb.start();
        InputStreamReader errStream = new InputStreamReader(
                proc.getInputStream());
        int ret = proc.waitFor();
        if (ret == 0) {
            return;
        }
        else {
            StringBuffer sb = new StringBuffer();
            while (true) {
                char[] buf = new char[1024];
                int read = errStream.read(buf);
                if (read >= 0) {
                    sb.append(Arrays.copyOf(buf, read));
                }
                else {
                    break;
                }
            }
            throw new ValidationException(sb.toString());
        }
    }

    public static void validateSeqUpload(final org.apache.hadoop.fs.Path pt,
            final String seqKey, final String meta)
            throws InterruptedException, IOException, ValidationException {

        ProcessBuilder pb = new ProcessBuilder();
        pb.command(RuntimeConfiguration.getInstance().getValidatorPath(),
                pt.toString(), seqKey, meta);

        Validator.doValidate(pb);
    }

    public static void validateUpload(final org.apache.hadoop.fs.Path pt,
            final String meta) throws IOException, InterruptedException,
            ValidationException {
        ProcessBuilder pb = new ProcessBuilder();
        pb.command(RuntimeConfiguration.getInstance().getValidatorPath(),
                pt.toString(), meta);

        Validator.doValidate(pb);
    }
}
