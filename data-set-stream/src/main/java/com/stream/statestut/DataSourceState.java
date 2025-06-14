package com.stream.statestut;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Random;

public class DataSourceState {
    public static void main(String[] args) throws IOException {
        ServerSocket listener = new ServerSocket(1234);
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection : "+socket.toString());
            BufferedReader br = new BufferedReader(new FileReader(""));

            try{
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
//                Random rand = new Random();
//                int count = 0;
//                int tenSum = 0;
                Date d = new Date();
                int x = 1;
                while (true){
                    int key = (x%2) +1;
                    String s = key + ","+ x;
                    x++;
                    System.out.println(s);
                    out.println(s);
                    Thread.sleep(50);
                }
            }finally {
                socket.close();;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
