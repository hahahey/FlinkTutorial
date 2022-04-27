package com.hahahey.wc;

import org.apache.flink.core.fs.Path;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author hahahey
 * @date 2022/1/3 18:16
 * @description:
 */
public class WordCountTest {

    @Test
    public void printPath() {
        Path path = new Path("/");
        System.out.println(path);

    }


}