package com.elad.wpmcn;

import org.junit.Test;

import java.io.IOException;

import static com.elad.wpmcn.GirlName.YUVAL;

/**
 * Created by eladw on 1/8/18.
 */
public class SchemeTestsV4 {

    /**
     * Create a file using a genericRecord
     * @throws IOException
     */
    @Test
    public void createFileV3() throws IOException {

        System.out.println("write to file");
        com.elad.wpmcn.MyPair myPair = new com.elad.wpmcn.MyPair().newBuilder()
                .setLeft("left")
                .setRight("right")
                .setIsValid(true)
                .setNameOfGirl(YUVAL)
                .build();
        //.set, com.elad.wpmcn.GirlName.YUVAL);
        AvroUtils.createFile(myPair, "/Users/eladw/git-dp/AvroExample/src/main/resources/output/ex4", "MyPairOutput-V4.bin");
        System.out.println("write stream");
    }
}
