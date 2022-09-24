import java.net.*;
import java.io.*;
class MyClient{
    public static void main(String args[])throws Exception{
        Socket socket = new Socket("localhost",3333);
        byte[] message = new byte[1024 * 1024 * 1024];
        socket.getOutputStream().write(message);
        socket.close();
    }}