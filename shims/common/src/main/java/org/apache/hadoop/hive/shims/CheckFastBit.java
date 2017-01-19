package org.apache.hadoop.hive.shims;

import gov.lbl.fastbit.FastBit;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * Created by malmee on 1/15/17.
 */
public final class CheckFastBit {
    static final Logger LOG = LoggerFactory.getLogger(HadoopShimsSecure.class);
    private static boolean isFastBit;

    private CheckFastBit() {
        isFastBit = false;
    }

    public static boolean getisFastBit() {
        return isFastBit;
    }

    public static void setisFastBit(boolean value) {
        isFastBit = value;
    }

    public static void buildIndexes(String value, String rowSchema) {
        Path path = new Path(value);
        Path parent = path.getParent();
        try {
            LOG.info("Building FastBit indexes");
            String columninfo=rowSchema;
            Runtime.getRuntime().exec("/home/malmee/FYP/NewHive2/fastbit/ardea/ardea -d " + parent + " -m "+columninfo+ " -t " + path);
            FastBit fastbit = new FastBit("");
            String opt = "index=<binning none/>";
            fastbit.build_indexes(parent.getName(),opt);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
