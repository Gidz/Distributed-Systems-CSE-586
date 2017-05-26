package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by gideon on 25/04/17.
 */

public class SimpleDynamoStorageHelper extends SQLiteOpenHelper {
        public static final String DATABASE_NAME = "ChordStorage.db";
    public static final String TABLE_NAME = "Stuff";
    public static final String COL1 = "key";
    public static final String COL2 = "value";

    static final String TAG = SimpleDynamoStorageHelper.class.getSimpleName();

    SQLiteDatabase db;
    ReentrantLock lock = new ReentrantLock();

    public SimpleDynamoStorageHelper(Context context) {
        super(context, DATABASE_NAME, null, 1);
        db = this.getWritableDatabase();
//        db.close();
    }


    @Override
    public void onCreate(SQLiteDatabase db) {
//        db.execSQL("CREATE TABLE " + TABLE_NAME + " (key TEXT UNIQUE PRIMARY KEY, value TEXT)");
        db.execSQL("CREATE TABLE " + TABLE_NAME + " (key TEXT , value TEXT, Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
        onCreate(db);
    }

    public boolean insertData(ContentValues cv) {
//    try {
//        count++;
//    } finally {
//        lock.unlock();
//    }
        try
        {
//            lock.lock();
            db = this.getWritableDatabase();
            String key = cv.getAsString("key");
            String value = cv.getAsString("value");

        //Making sure that rows with same key will be updated to the latest value
//            db.execSQL("DELETE FROM " + TABLE_NAME + " WHERE EXISTS(SELECT * FROM " + TABLE_NAME + " WHERE KEY=\"" + key + "\")");

        if (db.insert(TABLE_NAME, null, cv) == -1) {
//            db.close();
            Log.e(TAG,"INSERT FAILED :(");
            return false;
        } else {
            Log.e(TAG,"Inserted the key "+ cv.getAsString("key")+" successfully");
//            db.close();
            return true;
        }
        }
        finally {
//            lock.unlock();
        }

    }

    public void deleteData(String key)
    {
        String query = null;
        db = this.getWritableDatabase();
        if (key.equals("*"))
        {
            query = "DELETE FROM " + TABLE_NAME;
        }
        else if(key.equals("@"))
        {
            query = "DELETE FROM " + TABLE_NAME;
        }
        else
        {
            query = "DELETE FROM "+TABLE_NAME+" WHERE key=\""+key+"\"";
        }
        db.execSQL(query);
//        db.close();
    }

    public MatrixCursor getData(String key) {
        Cursor temp;
        db = this.getReadableDatabase();
        String QUERY = null;
        if (key.equals("@"))
        {
            QUERY = "SELECT key,value FROM " + TABLE_NAME;
        }
        else if (key.equals("*"))
        {
            QUERY = "SELECT key,value FROM " + TABLE_NAME;
        }
        else
        {
//            QUERY = "SELECT key,value FROM " + TABLE_NAME + " WHERE key='" + key + "'";
            QUERY = "SELECT key,value FROM " + TABLE_NAME + " WHERE key='" + key + "'"+" ORDER BY Timestamp DESC LIMIT 1";
        }
        temp = db.rawQuery(QUERY, null);
        MatrixCursor toSend = new MatrixCursor(new String[]{"key", "value"});
        String[] values = new String[2];
        temp.moveToFirst();
        try
        {
            if (temp != null) {
                do {
                    for (int i = 0; i < temp.getColumnCount(); i++) {

                        if(i%2==0)
                        {
                            if(temp.getString(i)!=null)
                                values[0] = temp.getString(i);
                        }
                        else if(i%2!=0)
                        {
                            values[1] = temp.getString(i);
                            toSend.addRow(values);
                            values[0]=null;
                            values[1]=null;
                        }
                    }
                } while (temp.moveToNext());
            }
        }
        catch (CursorIndexOutOfBoundsException e)
        {
            Log.e(TAG,e.toString());
        }


        Log.e(TAG,"RUN STARTS");
        if(toSend.moveToFirst() && toSend!=null){
            do{
                // YOUR CODE FOR EACH CURSOR ITEM HERE.
                // the cursor moves one field at a time through it's whole dataset
                for(int i=0;i<toSend.getColumnCount();i++)
                {
                    Log.e(TAG,">"+toSend.getString(i) );
                }
            }while(toSend.moveToNext());
        }
        toSend.moveToFirst();
        Log.e(TAG,"RUN ENDS");
        temp.close();
        return toSend;
    }
}
