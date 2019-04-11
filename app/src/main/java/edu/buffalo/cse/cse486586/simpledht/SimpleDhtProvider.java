package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.Scanner;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    public static final String TAG = SimpleDhtProvider.class.getSimpleName();
    public static final int SERVER_PORT = 10000;
    public static String MasterPort = "11108";
    public static String myPort, endPort;
    public static String myUri = "edu.buffalo.cse.cse486586.simpledht.provider";

    public static class MyAvd{
        String myPort, myId, myHash;

        public MyAvd(String myPort, String myId, String myHash) {
            this.myPort = myPort;
            this.myId = myId;
            this.myHash = myHash;
        }
    }


    public class AvdCompare implements Comparator<MyAvd> {

        @Override
        public int compare(MyAvd lhs, MyAvd rhs) {
            return lhs.myHash.compareTo(rhs.myHash);
        }
    }


    private ArrayList<MyAvd> avds_list = new ArrayList<MyAvd>();
    public static MyAvd curr;
    public static String prev, next;


    public static Uri getUri() {
        Uri.Builder builder = new Uri.Builder();
        builder.authority(myUri);
        builder.scheme("content");
        Uri uri = builder.build();
        return uri;
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        if(selection.equals("@") || selection.equals("*")) {
            Log.v("Delete:", "Inside the delete");
            File files[] = SimpleDhtActivity.context.getFilesDir().listFiles();
            for (File file : files) {
                file.delete();
            }
        }
        else {
            File files[] = SimpleDhtActivity.context.getFilesDir().listFiles();
            for (File file : files) {
                if(file.getName().equals(selection))
                    file.delete();
            }
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String key = values.getAsString("key");
        String value = values.getAsString("value");
        boolean currAvd = false;
        try {
            if(prev != null) {
                if (genHash(key).compareTo(curr.myHash) < 0 && genHash(key).compareTo(genHash(String.valueOf(Integer.parseInt(prev)/2))) > 0) {
                    currAvd = true;
                }
            }
            if(prev == null && next == null) {
                currAvd = true;
            }
            if( prev != null) {
                if (curr.myHash.compareTo(genHash(String.valueOf(Integer.parseInt(prev)/2))) < 0 && (genHash(key).compareTo(curr.myHash) < 0 || genHash(key).compareTo(genHash (String.valueOf(Integer.parseInt(prev)/2))) > 0)) {
                    currAvd = true;
                }
            }
            if (currAvd) {
                Log.e("Insert in avd with","key = "+  genHash(key));
                DataOutputStream dos = new DataOutputStream(getContext().openFileOutput(key, Context.MODE_PRIVATE));
                dos.writeUTF(value);
            } else {
                String msg = "Insert\n" + key + "\n" + value;
                Log.e("Call 2 insrt with keys:" , key);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, next);
            }
        } catch (IOException ioe) {
            Log.e(TAG,"I/O Exception");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        Log.e("Start", "Inside onCreate function");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPortStr = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.e("My port", myPortStr);

        myPort = myPortStr;
        endPort = myPortStr;
        try {
            curr = new MyAvd(myPort,portStr, genHash(portStr));
            avds_list.add(curr);
            Log.e("try", "Added");
            Collections.sort(avds_list, new AvdCompare());
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

            if (!myPort.equals(MasterPort)) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Join:1\n" + curr.myPort + "\n" + MasterPort, MasterPort);
            }

            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch(Exception ex) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
        return false;
    }


    private void JoinClient(String message, String portNumber) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message,portNumber);

    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            try {
                ServerSocket serverSocket = sockets[0];
                Scanner scr = null;
                PrintWriter ptr = null;
                while (true) {
                    Socket socket = serverSocket.accept();
                    scr = new Scanner(socket.getInputStream());
                    //DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    ptr = new PrintWriter(socket.getOutputStream(), true);
                    String command = scr.nextLine();
                    if (command.contains("Join")) {
                        Log.e("New Node", "Incoming new node in the ring");
                        int n = Integer.parseInt(command.split(":")[1]);
                        for (int i = 0; i < n; i++) {
                            String portId = scr.nextLine() ;
                            String id = String.valueOf(Integer.parseInt(portId)/2);
                            MyAvd myAvd = new MyAvd(portId, id, genHash(id));
                            avds_list.add(myAvd);
                            Collections.sort(avds_list, new AvdCompare());
                        }
                        String currentAvdPred, currentAvdSucc;
                        int totalNodes = avds_list.size();
                        for(int i = 0; i < totalNodes; i++ ) {
                            MyAvd currAvd = avds_list.get(i);
                            if (i == totalNodes - 1) {
                                currentAvdSucc = avds_list.get(0).myPort;
                            } else {
                                currentAvdSucc = avds_list.get(i + 1).myPort;
                            }
                            if (i == 0) {
                                currentAvdPred = avds_list.get(totalNodes - 1).myPort;
                            } else {
                                currentAvdPred = avds_list.get(i - 1).myPort;
                            }
                            JoinClient("Update\n" + currentAvdPred + "\n" + currentAvdSucc, currAvd.myPort);
                        }
                    }
                    else if (command.contains("Update")) {
                        Log.e("Inside the update cmd", command);
                        int n = command.split("\n").length;
                        for (int i = 0; i < n; i++) {
                            String myPred = scr.nextLine();
                            String mySucc = scr.nextLine();
                            prev = myPred;
                            Log.e("My Predecessor: ", prev);
                            next = mySucc;
                            Log.e("My Successor: ", next);
                        }
                    }
                    else if (command.contains("Insert")) {
                        Log.e("Inside the Insert cmd", command);
                        String k = scr.nextLine();
                        String v = scr.nextLine();
                        ContentValues contentValues = new ContentValues();
                        contentValues.put("key", k);
                        contentValues.put("value", v);
                        getContext().getContentResolver().insert(getUri(), contentValues);
                    }
                    else if (command.contains("Query")) {
                        Log.e("Reached Server Port: " + myPort, "Query Command");
                        if (command.contains("*")) {
                            endPort =command.split(":")[1];
                            Log.e("EndGame", "End Port is: " + endPort);
                        }
                        if (!command.contains(String.valueOf(myPort))) {
                            String key = scr.nextLine();
                            Cursor cursor = SimpleDhtActivity.mContentResolver.query(getUri(),null, key, null, null);
                            StringBuilder message = new StringBuilder();
                            if (cursor == null || cursor.getCount() < 1) {
                                message.append("empty");
                            } else {
                                ptr = new PrintWriter(socket.getOutputStream(), true);
                                while (cursor.moveToNext()) {
                                    String retrieveKey = cursor.getString(cursor.getColumnIndex("key"));
                                    String retrieveValue = cursor.getString(cursor.getColumnIndex("value"));
                                    message.append(retrieveKey + "\n" + retrieveValue + "\n");
                                }
                            }
                            //dos.writeUTF(message.toString());
                            //dos.flush();
                            ptr.println(message.toString());
                            ptr.flush();
                        } else {
                            /*dos = new DataOutputStream(socket.getOutputStream());
                            dos.writeBytes("stop");
                            dos.flush();*/
                            ptr = new PrintWriter(socket.getOutputStream(), true);
                            ptr.println("stop");
                            ptr.flush();
                        }
                    }
                    ptr.close();
                    scr.close();
                }
            } catch (IOException e) {
                Log.e(TAG,"I/O Exception");
            } catch (NoSuchAlgorithmException e){
                e.printStackTrace();
            }
            return null;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {

            String msgToSend = strings[0];
            String myport1 = strings[1];
            Log.e("Client - msg", "Inside CT sending message");
            try {
                // Reference: PA1 code Snippet
                Socket socket = new Socket(InetAddress.getByAddress(new byte[] { 10, 0, 2, 2 }), Integer.parseInt(myport1));
                PrintWriter ptr = new PrintWriter(socket.getOutputStream(), true);
                ptr.println(msgToSend);
                ptr.flush();
                Log.e("///////ack", "//////Send Ack");
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        try {
            Log.e("Function", "Inside query ab");
            String columnNames[] = new String[]{"key", "value"};
            MatrixCursor matrixCursor = new MatrixCursor(columnNames);
            if (selection.equals("@")) {
                for (File file : SimpleDhtActivity.context.getFilesDir().listFiles()) {
                    DataInputStream dis = new DataInputStream(SimpleDhtActivity.context.openFileInput(file.getName()));
                    String value = dis.readUTF();
                    columnNames = new String[]{file.getName(), value};
                    matrixCursor.addRow(columnNames);
                }
                return matrixCursor;
            }
            else if (selection.contains("*")) {
                if (selection.equals("*")) {
                    endPort = myPort;
                }
                File files[] = SimpleDhtActivity.context.getFilesDir().listFiles();
                for (File file : files) {
                    DataInputStream dis = new DataInputStream(SimpleDhtActivity.context.openFileInput(file.getName()));
                    String value = dis.readUTF();
                    columnNames = new String[]{file.getName(), value};
                    matrixCursor.addRow(columnNames);
                }
                String value = "";
                String navd = next;
                if (navd != null) {
                    String msg = "Query:" + endPort + ":**" + "\n" + "**";
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(navd));
                    PrintWriter ptr = new PrintWriter(socket.getOutputStream(), true);
                    ptr.println(msg);
                    ptr.flush();
                    //DataInputStream dis = new DataInputStream(socket.getInputStream());
                    Scanner scr = new Scanner(socket.getInputStream());
                    String key1 = scr.nextLine();
                    if (key1.contains("empty")) {
                        Log.e("Text: ", "Null returned");
                    } else if (!key1.contains("stop")) {
                        value = scr.nextLine();
                        matrixCursor.addRow(new String[]{key1, value});
                        while (scr.hasNext()) {
                            key1 = scr.nextLine();
                            value = scr.nextLine();
                            matrixCursor.addRow(new String[]{key1, value});
                        }
                    }
                    scr.close();
                    socket.close();
                    return matrixCursor;
                }
                return matrixCursor;
            }
            else {
                boolean fileRet = false;
                Log.e("Single query", "Yo single query");
                File files[] = SimpleDhtActivity.context.getFilesDir().listFiles();
                for (File file : files) {
                    if (file.getName().equals(selection)) {
                        fileRet = true;
                        Log.e("Info: ", "File found");
                        DataInputStream dis = new DataInputStream(SimpleDhtActivity.context.openFileInput(file.getName()));
                        String val = dis.readUTF();
                        columnNames = new String[]{file.getName(), val};
                        matrixCursor.addRow(columnNames);
                        return matrixCursor;
                    }
                }
                if(fileRet == false) {
                    Log.e("Info", "File not found,ready to ping next");
                    String val = "";
                    String navd = next;
                    if(navd != null) {
                        String msg = "Query:" + myPort + "\n" + selection;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(navd));
                        PrintWriter ptr = new PrintWriter(socket.getOutputStream(), true);
                        ptr.println(msg);
                        ptr.flush();
                        Scanner scr = new Scanner(socket.getInputStream());
                        scr.nextLine();
                        val = scr.nextLine();
                        scr.close();
                        socket.close();
                        matrixCursor.addRow(new String[]{selection, val});
                        return matrixCursor;
                    }
                }
            }
        } catch (IOException ioe) {
            Log.e(TAG, "I/O Exception");
        }
        return null ;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}