package edu.buffalo.cse.cse486586.groupmessenger2;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by gideon on 21/02/17.
 */

//A Database helper class to use with the Content Provider
public class MessageStoreHelper extends SQLiteOpenHelper {
    public static final String DATABASE_NAME = "MessagesStore.db";
    public static final String TABLE_NAME = "Messages";
    public static final String COL1 = "key";
    public static final String COL2 = "value";

    SQLiteDatabase db;

    public MessageStoreHelper(Context context) {
        super(context, DATABASE_NAME, null, 1);
        db = this.getWritableDatabase();
        db.close();
    }


    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE " + TABLE_NAME + " (key TEXT UNIQUE PRIMARY KEY, value TEXT)");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
        onCreate(db);
    }

    public boolean insertData(ContentValues cv) {
        db = this.getWritableDatabase();
        String key = cv.getAsString("key");
        String value = cv.getAsString("value");

        //Making sure that rows with same key will be updated to the latest value
        db.execSQL("DELETE FROM " + TABLE_NAME + " WHERE EXISTS(SELECT * FROM " + TABLE_NAME + " WHERE KEY=\"" + key + "\")");
        ;

        if (db.insert(TABLE_NAME, null, cv) == -1) {
            db.close();
            return false;
        } else {
            db.close();
            return true;
        }
    }

    public MatrixCursor getData(String key) {
        Cursor temp;
        db = this.getReadableDatabase();
        temp = db.rawQuery("SELECT key,value FROM " + TABLE_NAME + " WHERE key='" + key + "'", null);
        MatrixCursor toSend = new MatrixCursor(new String[]{"key", "value"});
        String[] values = new String[2];
        temp.moveToFirst();
        if (temp != null) {
            do {
                for (int i = 0; i < temp.getColumnCount(); i++) {
                    values[i] = temp.getString(i);
                }
            } while (temp.moveToNext());
        }
        temp.close();
        toSend.addRow(values);
        return toSend;
    }
}
