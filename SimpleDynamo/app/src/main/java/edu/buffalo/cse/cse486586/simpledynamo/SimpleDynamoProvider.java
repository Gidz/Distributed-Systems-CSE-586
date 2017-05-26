package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleDynamoProvider extends ContentProvider {

    ReentrantLock lock = new ReentrantLock();
    SimpleDynamoStorageHelper sdHelper;
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static int myPort, emulatorNumber;
    static int successorPlusOne = -1, successor = -1;
    static int failedPort=-1;
    static String node_id;
    static String QUERY_DATA = "";
    static Queue<String> lockerRoom = new LinkedList<String>();
    Uri contentProviderUrl = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamno.provider");

    //To be used for node joins and stuff.User defined class. Extends ArrayList
    static CircularList<String> nodes = new CircularList<String>();

    //Gives the emulator number once port number is given
    public int resolveDevice(int p) {
        if (p == 11108) {
            return 5554;
        } else if (p == 11112) {
            return 5556;
        } else if (p == 11116) {
            return 5558;
        } else if (p == 11120) {
            return 5560;
        } else if (p == 11124) {
            return 5562;
        } else {
            return -1;
        }
    }

    //Gives the port number once emulator number is given
    public int resolvePort(int e) {
        if (e == 5554) {
            return 11108;
        } else if (e == 5556) {
            return 11112;
        } else if (e == 5558) {
            return 11116;
        } else if (e == 5560) {
            return 11120;
        } else if (e == 5562) {
            return 11124;
        } else {
            return -1;
        }

    }

    String getHashFromPort(int p) {
        if (p == 11108) {
            return "33d6357cfaaf0f72991b0ecd8c56da066613c089";
        } else if (p == 11112) {
            return "208f7f72b198dadd244e61801abe1ec3a4857bc9";
        } else if (p == 11116) {
            return "abf0fd8db03e5ecb199a9b82929e9db79b909643";
        } else if (p == 11120) {
            return "c25ddd596aa7c81fa12378fa725f706d54325d12";
        } else if (p == 11124) {
            return "177ccecaec32c54b82d5aaafc18a2dadb753e3b1";
        } else {
            return "";
        }

    }

    int getPortFromHash(String hash) {
        if (hash.equals("33d6357cfaaf0f72991b0ecd8c56da066613c089")) {
            return 11108;
        } else if (hash.equals("208f7f72b198dadd244e61801abe1ec3a4857bc9")) {
            return 11112;
        } else if (hash.equals("abf0fd8db03e5ecb199a9b82929e9db79b909643")) {
            return 11116;
        } else if (hash.equals("c25ddd596aa7c81fa12378fa725f706d54325d12")) {
            return 11120;
        } else if (hash.equals("177ccecaec32c54b82d5aaafc18a2dadb753e3b1")) {
            return 11124;
        } else {
            return 0;
        }

    }


    public static MatrixCursor putDataIntoCursor(String input) {
        StringTokenizer slasher = new StringTokenizer(input, ";");
        MatrixCursor toSend = new MatrixCursor(new String[]{"key", "value"});
        String[] values = new String[2];
        while (slasher.hasMoreElements()) {
            values[0] = slasher.hasMoreElements() ? slasher.nextElement().toString() : null;
            values[1] = slasher.hasMoreElements() ? slasher.nextElement().toString() : null;
            toSend.addRow(values);
            values[0] = values[1] = "";

        }
        return toSend;
    }

    //TODO: onCreate() Method
    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
        SimpleDynamoStorageHelper sdHelper = new SimpleDynamoStorageHelper(getContext());
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = (Integer.parseInt(portStr) * 2);
        emulatorNumber = resolveDevice(myPort);
        try {
            node_id = genHash(String.valueOf(emulatorNumber));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        String[] fixed = {"177ccecaec32c54b82d5aaafc18a2dadb753e3b1","208f7f72b198dadd244e61801abe1ec3a4857bc9","33d6357cfaaf0f72991b0ecd8c56da066613c089","abf0fd8db03e5ecb199a9b82929e9db79b909643","c25ddd596aa7c81fa12378fa725f706d54325d12"};
        for (int i=0;i<5;i++)
        {
            nodes.add(fixed[i]);
        }

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            //Do nothing
        }
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        Log.e(TAG, "Server thread started !");
        Log.e(TAG, nodes.toString());
        Log.e(TAG,"I identify myself as "+node_id);
        for (int i=0;i<nodes.size();i++)
        {
            if(node_id.equals(nodes.get(i))) {
                successor = getPortFromHash(nodes.get((i + 1)));
                successorPlusOne = getPortFromHash(nodes.get((i + 2)));
            }
        }
        Log.e(TAG,"Successor and successorPlusOne are "+getHashFromPort(successor)+","+getHashFromPort(successorPlusOne)+" respectively");
        sendToAll("ALIVE;"+myPort);
        return true;
    }

    //TODO : delete() method
    public void deleteNow(String selection)
    {
        sdHelper = new SimpleDynamoStorageHelper(getContext());
        sdHelper.getWritableDatabase();
        sdHelper.deleteData(selection);
    }
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        String location = null;
        if (selection.equals("*")) {
            //Send delete all call to everyone
            sendToAll("DELETE_ALL");
        } else if (selection.equals("@")) {
            sdHelper.deleteData("*");
        } else {
            try {
                location = locateKey(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        int position = -1;
        for (int i=0;i<nodes.size();i++)
        {
            if(location.equals(nodes.get(i)))
            {
                position = i;
            }
        }
        String toSend = "DELETE;" + selection;
         //TODO: Open a socket and send directly
        sendMessage(toSend,getPortFromHash(nodes.get(position)));
        sendMessage(toSend,getPortFromHash(nodes.get(position+1)));
        sendMessage(toSend,getPortFromHash(nodes.get(position+2)));
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    //TODO: insert() method
    public synchronized  void insertNow(ContentValues values)
    {
        sdHelper = new SimpleDynamoStorageHelper(getContext());
        sdHelper.getWritableDatabase();
        sdHelper.insertData(values);
//        sdHelper.close();
    }
    @Override
    public Uri insert(Uri uri, ContentValues values) {
  /*
   * column and a value column) and one row that contains the actual (key, value) pair to be
   * inserted.
   *
   * For actual storage, you can use any option. If you know how to use SQL, then you can use
   * SQLite. But this is not a requirement. You can use other storage options, such as the
   * internal storage option that we used in PA1. If you want to use that option, please
   * take a look at the code for PA1.
   */
        try
        {
            lock.lock();
            sdHelper = new SimpleDynamoStorageHelper(getContext());
        sdHelper.getWritableDatabase();
        String key = (String) values.get("key");
        String value = (String) values.get("value");

        String location = null;
        //        insert(contentProviderUrl,values);
        try {
            location = locateKey(key);
            Log.e(TAG, "The key should be placed at " + location);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        int position = -1;
        for (int i=0;i<nodes.size();i++)
        {
            if(location.equals(nodes.get(i)))
            {
                position = i;
            }
        }
            String toSend = "INSERT;" + key + ";" + value;
         //TODO: Open a socket and send directly

        Log.v("insert", values.toString());

            if(location.equals(node_id))
            {
                insertNow(values);
                new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend, Integer.toString(getPortFromHash(nodes.get(position+1))));
                new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend, Integer.toString(getPortFromHash(nodes.get(position+2))));
            }
            else
            {
                new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend, Integer.toString(getPortFromHash(nodes.get(position))));
                new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend, Integer.toString(getPortFromHash(nodes.get(position+1))));
                new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend, Integer.toString(getPortFromHash(nodes.get(position+2))));
            }

        //Unfold the content values
        return uri;
        }
        finally {
            lock.unlock();
        }

    }

    //TODO: query() method
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
  /*
   * You need to implement this method. Note that you need to return a Cursor object
   * with the right format. If the formatting is not correct, then it is not going to work.
   *
   * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
   * still need to be careful because the formatting might still be incorrect.
   *
   * If you use a file storage option, then it is your job to build a Cursor * object. I
   * recommend building a MatrixCursor described at:
   * http://developer.android.com/reference/android/database/MatrixCursor.html
   */

        //Using a Matrix cursor
        try
        {
           lock.lock();
            MatrixCursor dataToReturn = null;
        sdHelper = new SimpleDynamoStorageHelper(getContext());
        sdHelper.getReadableDatabase();
        String location = "";
        String ship = "";
        Log.e(TAG,"Trying to query "+selection);
        //A workaround for chrod till it can be implemented .. Got lost in the chaos of networking
        if (selection.equals("*")) {
            for (int i = 0; i < nodes.size(); i++) {
                int[] ports = {11108,11112,11116,11120,11124};
                String toSend = "QUERY;" + "@";
                AsyncTask<String, String, String> a = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend, Integer.toString(ports[i]));
                String aa = "";
                try {
                    aa = a.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                Log.e(TAG, "Got the data " + aa);
                if (!aa.equals("Done"))
                {
                    ship += ";" + aa;
                }
            }
            //when using publish progress, unable to receive the data from other side.. So came up with this.
            return putDataIntoCursor(ship);
        } else {
            if (selection.equals("@")) {
                return sdHelper.getData(selection);
            } else {
                try {
                    location = locateKey(selection);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                if (location.equals(node_id)) {
                    //Using a Matrix cursor
                    Log.e(TAG, "Data is on the same node!!");
                    return sdHelper.getData(selection);
                } else {
                    AsyncTask<String, String, String> a = null;
                    Log.e(TAG, "Data is not on the same node!!");

                    String toSend = "QUERY;" + selection;

                    int position=-1;
                        for (int i=0;i<nodes.size();i++)
                        {
                            if(location.equals(nodes.get(i)))
                            {
                               position = i;
                            }
                        }
                        Log.e(TAG,"Trying to retrieve "+selection+" The data should reside on "+ location +"The node probably failed. Data will be retrieved from the successor");



                    try {
                        a = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend, Integer.toString(getPortFromHash(nodes.get(position))));
                        String aa = a.get();
                        Log.e(TAG," 1 : The QUERY DATA has the following "+QUERY_DATA);
                        if(aa.equals("Done"))
                        {
                            a = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend, Integer.toString(getPortFromHash(nodes.get(position+1))));
                            aa = a.get();
                            Log.e(TAG," 2 : The QUERY DATA has the following "+QUERY_DATA);
                            if (aa.equals("Done"))
                            {
                                a = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend, Integer.toString(getPortFromHash(nodes.get(position+2))));
                                aa = a.get();
                                Log.e(TAG," 3 : The QUERY DATA has the following "+QUERY_DATA);
                            }
                        }
                        Log.e(TAG,"Received "+aa);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                    Log.e(TAG, "Looks like I succeeded " + QUERY_DATA);
                    //Unfold the data, place it in cursor and return
                    dataToReturn = putDataIntoCursor(QUERY_DATA);
                    if (QUERY_DATA.equals("Done"))
                    {
                       Log.e(TAG,"Failed to retrieve the data for the key "+selection);
                    }
                    ///////

//                    dataToReturn.moveToFirst();
//
//                    if (dataToReturn != null) {
//                        do {
//                            for (int i = 0; i < dataToReturn.getColumnCount(); i++) {
//                                Log.e(TAG, "->" + dataToReturn.getString(i));
//                            }
//                        } while (dataToReturn.moveToNext());
//                    }
//

                    ///////
                    return dataToReturn;
                }
            }
        }
        }
        finally {
            lock.unlock();
        }

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static String locateKey(String key) throws NoSuchAlgorithmException {
        String hashKey = genHash(key);
        String location = null;
        Log.e(TAG, "If all fails, place this at" + nodes.get(0));
        for (int i = 0; i < nodes.size(); i++) {
            if (hashKey.compareTo(nodes.get(i)) < 0) {
                location = nodes.get(i);
                break;
            }
        }
        if (location == null) {
            location = nodes.get(0);
        }
        return location;
    }

    //TODO: Client Task
    private class ClientTask extends AsyncTask<String, String, String> {
        @Override
        protected String doInBackground(String... msgs) {
            Log.e(TAG, "Entered the client task!");
            StringTokenizer slasher = new StringTokenizer(msgs[0], ";");
            String messageType = slasher.nextToken();

            try {
                Socket sock = new Socket(InetAddress.getByAddress(new byte[]{
                        10,
                        0,
                        2,
                        2
                }), Integer.parseInt(msgs[1]));
                sock.setSoTimeout(2000);

                Log.e(TAG, "Created the socket!");

                //Create input and output streams
                DataOutputStream tunnelOut = new DataOutputStream(sock.getOutputStream());
                DataInputStream tunnelIn = new DataInputStream(sock.getInputStream());
                tunnelOut.writeUTF(msgs[0]);
                Log.e(TAG, "Sent out : " + msgs[0]);
                if (messageType.equals("QUERY")) {
//                    Reset the QUERY_DATA
                    QUERY_DATA="";
                    Log.e(TAG, "Waiting for the response . . ");
                    String receivedResponse = tunnelIn.readUTF();
                    Log.e(TAG, "Response received " + receivedResponse);
                    if(receivedResponse!=null)
                    {
                        if (QUERY_DATA.equals("")) {
                        QUERY_DATA += receivedResponse;
                    } else {
                        QUERY_DATA = "";
                        QUERY_DATA += receivedResponse;
                    }
                    }


//                    onPostExecute();
                    return QUERY_DATA;
                } else if (messageType.equals("QUERY_ALL")) {
                    Log.e(TAG, "Waiting for the response . . ");
                    String receivedResponse = tunnelIn.readUTF();
                    Log.e(TAG, "Response received " + receivedResponse);
                    return receivedResponse;
                }
                else if(messageType.equals("INSERT"))
                {
                    String receivedResponse = tunnelIn.readUTF();
                    Log.e(TAG, "Response received " + receivedResponse);
                }
            } catch (SocketTimeoutException e) {
                Log.e(TAG, "Socket timeout!");
            } catch (IOException e) {
                //Log.e(TAG, "The team has faced a problem with monkeys in the forest");
                //Until the time I figure out how to use SocketTimeoutException :/
                Log.e(TAG, "The node "+ resolveDevice(Integer.parseInt(msgs[1])) +" fuckin failed. Damn ! ");
                failedPort = Integer.parseInt(msgs[1]);
                if(messageType.equals("INSERT") || messageType.equals("DELETE"))
                {
                    lockerRoom.add(msgs[0]);
                    Log.e(TAG,"Just detected a failure. Storing the message in lockerRoom. Contents are as follows : "+lockerRoom.toString());
                }
                Log.e(TAG,"Marked the failed port");
                //Notify all nodes about the failure
                sendToAll("FAILED;"+failedPort);
            }
            return "Done";
        }

        protected void onProgressUpdate(String... strings) {
            //Nothing here
        }
    }

    //TODO: ServerTask
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            String strReceived = null;

            while (true) {
                ServerSocket serverSocket = sockets[0];
                Socket listener = null;
                try {

                    //Log.e(TAG,"Server Socket running successfully!");
                    listener = serverSocket.accept();
                    Log.e(TAG, "Listening . . ");
                    DataInputStream tunnelIn = new DataInputStream(listener.getInputStream());
                    DataOutputStream tunnelOut = new DataOutputStream(listener.getOutputStream());
                    strReceived = tunnelIn.readUTF();
                    Log.e(TAG, "Received " + strReceived);
                    StringTokenizer slasher = new StringTokenizer(strReceived, ";");
                    String messageType = slasher.nextToken();

                    if (messageType.equals("INSERT")) {
                            ContentValues cv = new ContentValues();
                            cv.put("key", slasher.nextToken());
                            cv.put("value", slasher.nextToken());
//                            insert(contentProviderUrl, cv);
                            insertNow(cv);
                            tunnelOut.writeUTF("PLACED THE IN PROPER PLACE!");
                        }
                    //TODO: When received Query from a Client
                    else if (messageType.equals("QUERY")) {
                        String query = slasher.nextToken();
                        String dataToReturn = "";
//                            Cursor temp = query(contentProviderUrl, null, query, null, null);
                            //
                        SimpleDynamoStorageHelper sdp = new SimpleDynamoStorageHelper(getContext());
                        Cursor temp = sdp.getData(query);

                            Log.e(TAG, "Dump out the data from the cursor!");
                            if (temp.moveToFirst() && temp != null) {
                                do {
                                    for (int i = 0; i < temp.getColumnCount(); i++) {
                                        Log.e(TAG, ">" + temp.getString(i));

                                        if (temp.isLast()) {
                                            if (i == 0) {
                                                dataToReturn += temp.getString(i) + ";";
                                            } else {
                                                dataToReturn += temp.getString(i);
                                            }

                                        } else {
                                            dataToReturn += temp.getString(i) + ";";
                                        }

                                    }
                                } while (temp.moveToNext());
                                tunnelOut.writeUTF(dataToReturn);
                            }

                            Log.e(TAG, "RUN ENDS");

                    }
                    else if (messageType.equals("DATA")) {
                        Log.e(TAG, "Received the data");
                    } else if (messageType.equals("DELETE")) {
                            deleteNow(slasher.nextToken());
                    } else if (messageType.equals("DELETE_ALL")) {
                            deleteNow("@");
                        }
                    else if(messageType.equals("FAILED"))
                    {
                       failedPort = Integer.parseInt(slasher.nextToken());
                        Log.e(TAG,"Update the failed port : "+failedPort);
                    }
                    else if (messageType.equals("ALIVE"))
                    {
                        int port = Integer.parseInt(slasher.nextToken());
                        if (failedPort == port)
                        {
                           failedPort = -1;
                           Log.e(TAG,"Removed from the failed nodes");
                            while(!lockerRoom.isEmpty())
                            {
                                String messageToSend = lockerRoom.poll();
                                StringTokenizer st = new StringTokenizer(messageToSend,";");
                                if(st.nextToken().equals("INSERT") || st.nextToken().equals("DELETE"))
                                {
                                    sendMessage(messageToSend,port);
                                }
                            }

                        }
                    }


                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NullPointerException e) {
                    //
                }
            }
        }

        protected void onProgressUpdate(String... strings) {
            //Updating the UI
            AsyncTask<String, String, String> a = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0], strings[1]);
            try {
                String aa = a.get();
                Log.e(TAG, "This should be returned " + aa);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }
    }
    //This class helps with using a list in circular form
    static class CircularList<Integer> extends ArrayList<Integer> {
        //Overriding the get function. If a value greater than the size of the list is asked for, it simply wraps around
        @Override
        public Integer get(int index) {
            return super.get(index % size());
        }
    }

    private void sendMessage(String message, int port)
    {
        if(port==failedPort)
        {
            lockerRoom.add(message);
            Log.e(TAG,"The locker room has the following messages "+lockerRoom.toString());
        }
        else
        {
            new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, Integer.toString(port));
        }

    }

    //A method to send a message to all the nodes
    private void sendToAll(String message)
    {
        int[] ports = {11108,11112,11116,11120,11124};
        for (int i=0;i<ports.length;i++)
        {
            sendMessage(message,ports[i]);
        }
    }
}
