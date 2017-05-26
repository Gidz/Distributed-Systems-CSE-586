package edu.buffalo.cse.cse486586.groupmessenger1;

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

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */
public class GroupMessengerActivity extends Activity {

    //This will be useful for the logging purposes
    static final String TAG = GroupMessengerActivity.class.getSimpleName();

    //Message sequence counter. I'm starting this from -1 because our keys will be named from 0. As soon as a message is received, counter is incremented by a value of 1
    static int counter = -1;

    //Declaring an array to hold all the ports
    static final String[] ports = {"11108", "11112", "11116", "11120", "11124"};

    //The server port number
    static final int SERVER_PORT = 10000;
    String messageToSend;
    Uri contentProvider;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        //Determining which AVD this is and what port number should be used tot send out messages
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

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
                messageToSend = textBox.getText().toString();

                //Resetting the text field as soon as a message is entered.
                textBox.setText("");

                //Calling the client task to send out the messages.
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToSend, myPort);

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

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            while (true) {
                ServerSocket serverSocket = sockets[0];

                try {
                    //Creating a listener socket and preparing it to start listening for messages
                    Socket listener = serverSocket.accept();
                    DataInputStream tunnelIn = new DataInputStream(listener.getInputStream());
                    String strReceived = tunnelIn.readUTF();

                    //Incrementing the counter
                    counter++;

                    //Updating the UI thread with the received message
                    publishProgress(counter + " " + strReceived);

                    ContentValues cv = new ContentValues();
                    cv.put("key", Integer.toString(counter));
                    cv.put("value", strReceived);

                    // Getting a Content Resolver and inserting the received message using it
                    contentProvider = getContentResolver().insert(Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger1.provider"), cv);

                    //Closing the DataInputStream
                    tunnelIn.close();
                    //Closing the listener socket
                    listener.close();
                } catch (Exception e) {
                    //For debugging purposes
                    Log.e(TAG, e.toString());
                }
            }
        }

        //This is an UI thread
        protected void onProgressUpdate(String... strings) {
            //Updating the UI
            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.append(strings[0] + "\n");

        }
    }

    //The Client Task
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            Socket socket = null;

            //Send out
            for (String remotePort : ports) {
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    DataOutputStream tunnelOut = new DataOutputStream(socket.getOutputStream());
                    Log.e(TAG, "Sent through the port number " + remotePort + " " + msgs[0]);
                    tunnelOut.writeUTF(msgs[0]);
                    Thread.sleep(100);
                    //Release the resources
                    tunnelOut.close();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }
}
