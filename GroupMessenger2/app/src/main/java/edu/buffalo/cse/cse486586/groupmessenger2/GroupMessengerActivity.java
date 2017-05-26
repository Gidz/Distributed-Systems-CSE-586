package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 * Umm.. and me as well :p
 */
public class GroupMessengerActivity extends Activity {

    //This will be useful for the logging purposes
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    //The server port number
    static final int SERVER_PORT = 10000;
    static int DEVICES = 5;
    static int currentDevice;
    static int myPort;
    int agreedSequence = -1;
    int proposedSequence = -1;
    int uid = 0;
    int finalCounter = -1;
    int failedPort = -1;
    //Declaring an array to hold all the ports
    ArrayList<String> ports = new ArrayList<String>(Arrays.asList("11108", "11112", "11116", "11120", "11124"));
    //A priority blocking queue for storing all the messages before their final delivery
    PriorityBlockingQueue<Message> priorityBlockingQueue = new PriorityBlockingQueue<Message>();
    String messageToSend;
    Uri contentProvider;

    //Takes in the port number and numbers the device.
    public int resolveDevice(int p) {

        if (p == 11108) {
            return 0;
        } else if (p == 11112) {
            return 1;
        } else if (p == 11116) {
            return 2;
        } else if (p == 11120) {
            return 3;
        } else if (p == 11124) {
            return 4;
        } else {
            return -1;
        }

    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        //Determining which AVD this is and what port number should be used tot send out messages
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = (Integer.parseInt(portStr) * 2);

        currentDevice = resolveDevice(myPort);

        //Creating a server socket to pass into the Server AsyncTask
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Creating a Server Socket Listener on a separate Async thread
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

  /*
   * TODO: Use the TextView to display your messages. Though there is no grading component
   * on how you display the messages, if you implement it, it'll make your debugging easier.
   */
        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

  /*
   * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
   * OnPTestClickListener demonstrates how to access a ContentProvider.
   */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

  /*
   * TODO: You need to register and implement an OnClickListener for the "Send" button.
   * In your implementation you need to get the message from the input box (EditText)
   * and send it to other AVDs.
   */
        Button sendButton = (Button) findViewById(R.id.button4);

        sendButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                EditText textBox = (EditText) findViewById(R.id.editText1);

                //Getting the message entered by the user in the EditText
                messageToSend = "message;" + textBox.getText().toString();

                //Resetting the text field as soon as a message is entered.
                textBox.setText("");

                //Calling the client task to send out the messages.
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToSend, Integer.toString(myPort));

            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }


    // Most of the code from here on is borrowed from the PA1 of this course
    // The Server Task
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        //This actual function which "delivers" the message i.e puts the message in the Content Provider
        private void deliverMessage(int sequence, String message) {
            ContentValues cv = new ContentValues();
            cv.put("key", Integer.toString(sequence));
            cv.put("value", message);
            // Getting a Content Resolver and inserting the received message using it
            contentProvider = getContentResolver().insert(Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger2.provider"), cv);
            publishProgress(finalCounter + " " + message);
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            String strReceived = null;

            while (true) {
                ServerSocket serverSocket = sockets[0];
                Socket listener = null;
                try {
                    listener = serverSocket.accept();
                    DataInputStream tunnelIn = null;
                    DataOutputStream tunnelOut = null;
                    tunnelIn = new DataInputStream(listener.getInputStream());
                    tunnelOut = new DataOutputStream(listener.getOutputStream());


                    String messageType = null;
                    String message = null;
                    int mid;
                    int fromDevice;
                    StringTokenizer st = null;


                    strReceived = tunnelIn.readUTF();
                    Log.e(TAG, "Received: " + strReceived);
                    st = new StringTokenizer(strReceived, ";");
                    //Check what type of message the server received
                    messageType = st.nextToken();


                    if (messageType.equals("message")) {
                        //TODO: What if Server receives a message
                        Log.e(TAG, "New message");
                        proposedSequence = Math.max(proposedSequence, agreedSequence) + 1;
                        //The actual message
                        message = st.nextToken();
                        mid = Integer.parseInt(st.nextToken());
                        fromDevice = Integer.parseInt(st.nextToken());
                        //Send out the proposal
                        tunnelOut.writeUTF("proposal;" + mid + ";" + proposedSequence);

                        //Create a message object and push it into the queue
                        Message m = new Message(message, mid, fromDevice, proposedSequence, currentDevice, false);
                        priorityBlockingQueue.add(m);
                        //Check the queue for any deliverables and deliver them
                        while (!priorityBlockingQueue.isEmpty() && priorityBlockingQueue.peek().getStatus()) {
                            Message tmp = priorityBlockingQueue.peek();
                            finalCounter++;
                            deliverMessage(finalCounter, tmp.getMessage());
                            priorityBlockingQueue.poll();
                            Log.e(TAG, "Delivered the message : " + tmp.getMessage());
                            publishProgress(tmp.getMessage());
                        }

                        Log.e(TAG, "proposal;" + mid + ";" + proposedSequence);

                    } else if (messageType.equals("agreed")) {
                        //TODO: What if Server receives an agreed message
                        int local_mid = Integer.parseInt(st.nextToken());
                        int local_fromDevice = Integer.parseInt(st.nextToken());
                        int a = Integer.parseInt(st.nextToken());
                        int suggestedBy = Integer.parseInt(st.nextToken());
                        agreedSequence = Math.max(agreedSequence, a);
                        //Check for the message, pull it out, modify the sequence value and then reinsert
                        Iterator<Message> itr = priorityBlockingQueue.iterator();
                        while (itr.hasNext()) {
                            Message tmp = (Message) itr.next();
                            if (tmp.getMid() == local_mid && tmp.getFromDevice() == local_fromDevice) {
                                priorityBlockingQueue.remove(tmp);
                                Log.e(TAG, "Removed this object");
                                tmp.setKey(a);
                                tmp.setSuggestedBy(suggestedBy);
                                tmp.setDeliverable();
                                priorityBlockingQueue.add(tmp);
                                Log.e(TAG, "Re-inserted the object");
                                Log.e(TAG, priorityBlockingQueue.peek().getData());
                            } else {
                                Log.e(TAG, "Didn't find anything in the fucking queue");
                            }
                        }
                        //Check the queue for any deliverables and deliver them
                        while (!priorityBlockingQueue.isEmpty() && priorityBlockingQueue.peek().getStatus()) {
                            Message tmp = priorityBlockingQueue.poll();
                            finalCounter++;
                            deliverMessage(finalCounter, tmp.getMessage());
                            Log.e(TAG, "Delivered the message : " + tmp.getMessage());
                        }
                    }
                    //If the message is informing about a failure
                    else if (messageType.equals("failure")) {
                        //Set the failure port to the value given by the message. Since it is so clear that there will be only one failure, this approach is enough
                        failedPort = Integer.parseInt(st.nextToken());

                        //TODO: What if Server receives a message informing failure
                        //Well this is not necessary but.. Yeah.. c'mon.. its fun..
                        Log.e(TAG, "Shame shame.. " + failedPort + ", now we all know that you crashed !");
                        Log.e(TAG, "We also know your device number.. it is " + resolveDevice(failedPort));

                        //Clean up the mess created by the failed node
                        Iterator<Message> itr = priorityBlockingQueue.iterator();
                        while (itr.hasNext()) {
                            Message tmp = (Message) itr.next();
                            //TODO: Handle the failure
                            if (tmp.getOrigin() == resolveDevice(failedPort)) {
                                priorityBlockingQueue.remove(tmp);
                            }
                        }
                    } else {
                        Log.e(TAG, "Unidentified packet");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }


        }

        protected void onProgressUpdate(String... strings) {
            //Updating the UI
            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.append(strings[0] + "\n");

        }
    }

    //The Client Task
    private class ClientTask extends AsyncTask<String, String, Void> {
        int[] proposals = {-1, -1, -1, -1, -1};

        @Override
        protected Void doInBackground(String... msgs) {
            //To increment the counter only once per message, we lock it as soon as the counter is incremented
            boolean lock = false;
            int suggestedBy = -1;
            int mid = -1;
            String msg = msgs[0];
            StringTokenizer st = new StringTokenizer(msg, ";");
            String messageType = st.nextToken();

            //The Client task can receive 2 kinds of inputs, a new message type or an agreed message type
            if (messageType.equals("message")) {
                String message = st.nextToken();
                //Iterate through all the ports
                for (int i = 0; i < DEVICES; i++) {
                    if (!(Integer.parseInt(ports.get(i)) == failedPort)) {
                        try {
                            SocketAddress sockaddr = null;
                            Socket sock = new Socket(InetAddress.getByAddress(new byte[]{
                                    10,
                                    0,
                                    2,
                                    2
                            }), Integer.parseInt(ports.get(i)));
                            //Set the socket timeout to 500ms
                            //Tried setting this to 100ms but then messages weren't ordered properly
                            sock.setSoTimeout(500);

                            //Create input and output streams
                            DataOutputStream tunnelOut = new DataOutputStream(sock.getOutputStream());
                            DataInputStream tunnelIn = new DataInputStream(sock.getInputStream());

                            if (lock == false) {
                                uid = uid + 1;
                                lock = true;
                            }
                            tunnelOut.writeUTF(msg + ";" + uid + ";" + currentDevice);
                            Log.e(TAG, "Sent out the message " + msg + " through the port " + ports.get(i));
                            //Read the proposal
                            String proposal = tunnelIn.readUTF();
                            Log.e(TAG, "Got proposal message " + proposal + " from the port " + ports.get(i));
                            StringTokenizer slasher = new StringTokenizer(proposal, ";");
                            String mType = slasher.nextToken();
                            if (mType.equals("proposal")) {
                                //TODO: Wait for the proposals and sending the agreed sequence
                                mid = Integer.parseInt(slasher.nextToken());
                                String s = slasher.nextToken();
                                proposals[i] = Integer.parseInt(s);
                            }
                        } catch (SocketTimeoutException e) {
                            Log.e(TAG, "Socket says that the connection died!");
                        } catch (IOException e) {
                            Log.e(TAG, "Nigga died.. Ain't responsing!!");
                            failedPort = Integer.parseInt(ports.get(i));
                            Log.e(TAG, "In future don't try to contact the port number " + failedPort);
                            publishProgress("failure;" + failedPort);
                        }
                    }
                }
                Log.e(TAG, "Caught all the proposals : " + Arrays.toString(proposals));
                int a = -1;
                for (int i = 0; i < proposals.length; i++) {
                    if (a < proposals[i]) {
                        a = proposals[i];
                        suggestedBy = i;
                        Log.e(TAG, "Found out who suggested this");
                    }
                }
                Log.e(TAG, "Agreed on " + a);
                //Create a new Client AsyncTask with the message of type agreed
                publishProgress("agreed;" + mid + ";" + currentDevice + ";" + a + ";" + suggestedBy);
            }
            //Logic to send out an agreed message
            else if (messageType.equals("agreed")) {
                Log.e(TAG, "This is a agreed sequence.");
                for (int i = 0; i < DEVICES; i++) {
                    try {
                        SocketAddress sockaddr = null;
                        Socket sock = new Socket(InetAddress.getByAddress(new byte[]{
                                10,
                                0,
                                2,
                                2
                        }), Integer.parseInt(ports.get(i)));
                        sock.setSoTimeout(100);
                        DataOutputStream tunnelOut = new DataOutputStream(sock.getOutputStream());
                        tunnelOut.writeUTF(msgs[0]);
                    } catch (IOException e) {

                    }
                }
            }
            //Logic to send out a message informing failure
            else if (messageType.equals("failure")) {
                for (int i = 0; i < DEVICES; i++) {
                    if (!(Integer.parseInt(ports.get(i)) == failedPort)) {
                        try {
                            SocketAddress sockaddr = null;
                            Socket sock = new Socket(InetAddress.getByAddress(new byte[]{
                                    10,
                                    0,
                                    2,
                                    2
                            }), Integer.parseInt(ports.get(i)));
                            sock.setSoTimeout(100);
                            DataOutputStream tunnelOut = new DataOutputStream(sock.getOutputStream());
                            tunnelOut.writeUTF(msgs[0]);
                            Log.e(TAG, "Sent out : " + msgs[0]);
                        } catch (IOException e) {

                        }
                    }
                }
            }
            //Well, this never happens
            else {
                Log.e(TAG, "Unidentified packet. No idea how to process this!");
            }

            return null;
        }

        protected void onProgressUpdate(String... strings) {
            //Create a new Client AsyncTask to deliver whatever is required
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0], Integer.toString(myPort));
        }
    }
}