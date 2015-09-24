package com.hhscyber.nl.zip;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author eve
 */
public class ZipTweetsMapper extends Mapper<LongWritable, Text, Text, Text> {

    // Alles wat je naar setup kunt verplaatsen....
    Text timestamp;

    public void setup(Context context) {
        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
        timestamp = new Text(this.getTimestampFromPath(filePathString));
        Configuration conf = context.getConfiguration();
//        if(conf.getStrings("filenames") != null)
//        {
//            String[] values = conf.getStrings("filenames");
//            String[] newval = new String[values.length+1];
//            for(int i= 0;i <= values.length;i++)
//            {
//                if(i != values.length)
//                {
//                   newval[i] = values[i];
//                }
//                else{
//                    newval[i] = this.getFileFromPath(filePathString);
//                }
//            }        
//        }
//        else{
//            conf.setStrings("filenames", this.getFileFromPath(filePathString));
//        }
        System.out.println("Timestamp " + timestamp);
    }

    @Override
    public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {  
        context.write(timestamp, val);
    }

    public String getTimestampFromPath(String path) {
        String[] tmp = path.split("/");
        if (tmp[6] != null) {
            return tmp[6];
        } else {
            return null;
        }
    }
    
    public String getFileFromPath(String path) {
        String[] tmp = path.split("/");
        if (tmp[7] != null) {
            return tmp[7];
        } else {
            return null;
        }
    }
    
    
}
