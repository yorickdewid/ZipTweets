package com.hhscyber.nl.zip;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author eve
 */
public class ZipTweetsReducer extends Reducer<Text, Text, NullWritable, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem hdfs = FileSystem.get(conf);
        Path baseOutputPath = new Path(conf.get("outputpath"));
        Path newFilePath = Path.mergePaths(baseOutputPath, new Path("/" + key + ".zip"));
        //String[] filenames = conf.getStrings("filenames");
        try {
            try (FSDataOutputStream fsOutStream = hdfs.create(newFilePath)) {
                ZipOutputStream zos = new ZipOutputStream(fsOutStream);
                for (Text value : values) {
                    java.util.Date date= new java.util.Date();
                    Timestamp t = new Timestamp(date.getTime());
                    StringBuilder sb = new StringBuilder();
                    sb.append(value.toString());
                    byte[] byt = sb.toString().getBytes();

                    ZipEntry zipEntry = new ZipEntry(t.toString()+".json");
                    zos.putNextEntry(zipEntry);

                    int length;
                    while ((length = (byt.length)) >= 0) {
                        zos.write(byt, 0, length);
                    }

                    zos.closeEntry();
                }
            }

            // nooit zomaar vage exceptions afvangen, bovenstaande gooit nooit een
            // exception, en bij outofmemory krijg je toch wel een stacktrace
        } catch (Exception ex) {
            Logger.getLogger(ZipTweetsReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
