package com.stream.statestut;
import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.Date;

public class Testing {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 1234;
        Random random = new Random();

        try (Socket socket = new Socket(host, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            System.out.println("Connected! Sending data...");

            for (int i = 0; i < 10; i++) {
                String message = System.currentTimeMillis() + "," + random.nextInt(100);
                out.println(message);
                System.out.println("Sent: " + message);
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
