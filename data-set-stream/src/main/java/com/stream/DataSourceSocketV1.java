package com.stream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class DataSourceSocketV1 {
    public static void main(String[] args) throws IOException {
        ServerSocket listener = new ServerSocket(1234);
        try{
            System.out.println("Got new connection:");
            Socket socket = listener.accept();
            System.out.println("Got new connection: "+ socket.toString());

            BufferedReader br = new BufferedReader(new FileReader(""));

            try{
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String line;
                int count=0;
                while ((line = br.readLine())!=null){
                    count++;
                    out.println(line);
                    if (count>=10){
                        count=0;
                        Thread.sleep(2000);
                    }else{
                        Thread.sleep(50);
                    }
                }
            } finally {
                socket.close();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
