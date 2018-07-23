package edu.umich.gaialib;

import com.google.common.primitives.Longs;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

public class TestFileWriter {

    public static void main(String[] args) throws IOException {

        String filename = "/tmp/test.wrt";

        System.out.println(filename.lastIndexOf('/'));
        System.out.println(filename.lastIndexOf('.'));
        System.out.println(filename.substring(filename.lastIndexOf('/'), filename.lastIndexOf('.')));

        ArrayList<String> str = new ArrayList<String>();
        str.add("asdf");
        str.add("fdsa");

        System.out.println(str);

        // first try to detect and create the folder

/*        RandomAccessFile dataFile1 = new RandomAccessFile(filename, "rw");

        dataFile1.setLength(20);



        RandomAccessFile dataFile2 = new RandomAccessFile(filename, "rw");

        dataFile2.setLength(30);


        dataFile2.seek(8);

        dataFile1.seek(1);

        dataFile2.write("1234".getBytes(), 0, 4);
        dataFile1.write("asdf".getBytes(), 0, 4);


        dataFile2.close();

        dataFile1.close();

        System.out.println(System.currentTimeMillis());
        Process p = Runtime.getRuntime().exec("sleep 3");
        System.out.println(System.currentTimeMillis());
        Process p2 = Runtime.getRuntime().exec("sleep 3");
        System.out.println(System.currentTimeMillis());
        try {
            p.waitFor();
            p2.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("" + System.currentTimeMillis());


        URL url = new URL("http://localhost:20020/tmp/test.wrt?start=0&len=5123412341231234");

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        // Useful?
        connection.setRequestProperty("Connection", "Keep-Alive");
        connection.setRequestProperty("Keep-Alive", "header");

        connection.connect();

        // then set up the connection, and convert the stream into queue
        DataInputStream input = new DataInputStream(connection.getInputStream());

        long size = connection.getHeaderFieldLong("x-FileLength" , -1);

        System.out.println("size" + size);

        System.out.println(input.read());
        System.out.println(input.read());
        System.out.println(input.read());
        System.out.println(input.read());*/
    }

}
