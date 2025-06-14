package com.stream;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Random;

public class DataSourceSocket {
    public static void main(String[] args) throws IOException {
        ServerSocket listener = new ServerSocket(9898);
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection : "+ socket.toString());

            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                Random rand = new Random();
                Date d = new Date();
                while(true){
                    int i = rand.nextInt(100);
                    String s = "" + System.currentTimeMillis()+","+i;
                    System.out.println(s);
                    out.println(s);
                    Thread.sleep(50);
                }
            } finally {
                socket.close();
            }

        }catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            listener.close();
        }
    }
}
