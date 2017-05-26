package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.util.StringTokenizer;

public class SimpleDynamoActivity extends Activity {
    static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    Uri contentProviderUrl = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
    @Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

		tv.setMovementMethod(new ScrollingMovementMethod());
//        findViewById(R.id.button3).setOnClickListener(
//                new OnTestClickListener(tv, getContentResolver()));

        findViewById(R.id.queryButton).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                TextView tv = (TextView) findViewById(R.id.textView1);
                EditText e = (EditText) findViewById(R.id.userInput);
                String s = e.getText().toString();
                //                getContentResolver().delete(contentProviderUrl, toDelete,null);
                Cursor result;
                try
                {
                    result = getContentResolver().query(contentProviderUrl, null,
                            s, null, null);
                    if (result.moveToFirst() && result != null) {
                        do {
                            // YOUR CODE FOR EACH CURSOR ITEM HERE.
                            // the cursor moves one field at a time trhough it's whole dataset
                            //                            Log.e(TAG,"Data retrieved is "+result.getString(0));
                            tv.append(result.getString(0) + ":" + result.getString(1) + "\n");
                        } while (result.moveToNext());
                    } else {
                        tv.append("No such data is found");
                    }
                }catch (NullPointerException e1)
                {
                    Log.e(TAG,"Seems No data has been returned");
                }

                e.setText("");
            }
        });

        findViewById(R.id.insertButton).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                EditText e = (EditText) findViewById(R.id.userInput);
                String s = e.getText().toString();
                StringTokenizer st = new StringTokenizer(s, ";");
                ContentValues cv = new ContentValues();
                cv.put("key", st.nextToken().toString());
                cv.put("value", st.nextToken().toString());
                getContentResolver().insert(contentProviderUrl, cv);

                e.setText("");

            }
        });


        findViewById(R.id.ldump).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                TextView tv = (TextView) findViewById(R.id.textView1);
                Cursor result = getContentResolver().query(contentProviderUrl, null,
                        "@", null, null);
                if (result.moveToFirst() && result != null) {
                    do {
                        // YOUR CODE FOR EACH CURSOR ITEM HERE.
                        // the cursor moves one field at a time through it's whole dataset
                        for (int i = 0; i < result.getColumnCount(); i++) {
                            //                            Log.e(TAG,"Data retrieved is "+result.getString(i) );
                            tv.append("Data retrieved is " + result.getString(i) + "\n");
                        }
                    } while (result.moveToNext());
                } else {
                    tv.append("No such data is found");
                }
                //TODO: Implement the LDUMP
            }
        });


        findViewById(R.id.gdump).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                TextView tv = (TextView) findViewById(R.id.textView1);
                Cursor result = getContentResolver().query(contentProviderUrl, null,
                        "*", null, null);
                try
                {
                    result = getContentResolver().query(contentProviderUrl, null,
                           "*", null, null);
                    if (result.moveToFirst() && result != null) {
                        do {
                            tv.append(result.getString(0) + ":" + result.getString(1) + "\n");
                        } while (result.moveToNext());
                    } else {
                        tv.append("No such data is found");
                    }
                }catch (NullPointerException e1)
                {
                    Log.e(TAG,"Seems No data has been returned");
                }
            }
        });

        findViewById(R.id.deleteButton).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                EditText e = (EditText) findViewById(R.id.userInput);
                String s = e.getText().toString();

                //                getContentResolver().insert(, cv);
                getContentResolver().delete(contentProviderUrl, s, null);
                e.setText("");

            }
        });

        findViewById(R.id.clearScreen).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                TextView tv = (TextView) findViewById(R.id.textView1);
                tv.setText("");
            }
        });
    }

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
