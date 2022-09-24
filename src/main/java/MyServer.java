import java.net.*;
import java.io.*;
class MyServer{
    public static void main(String args[])throws Exception{
        ServerSocket listener = new ServerSocket(3333);
        Socket socket=listener.accept();

        int count = 0;
        int read;
        long start = System.currentTimeMillis();
        byte[] buffer = new byte[32*1024];
        while ((read = socket.getInputStream().read(buffer)) != -1) {
            count++;
        }
        System.out.println("++++++count: " + count + " : " + (System.currentTimeMillis() - start));
        listener.close();
        socket.close();
    }
}