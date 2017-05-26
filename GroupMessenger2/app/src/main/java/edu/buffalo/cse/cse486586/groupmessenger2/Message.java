package edu.buffalo.cse.cse486586.groupmessenger2;

import static java.lang.Integer.compare;

/**
 * Created by gideon on 24/03/17.
 */


/*
 * The Priority Blocking Queue will store the Message objects
 * For comparisions betwwen 2 objects, Comparable interface is implemented
 */


@SuppressWarnings("ALL")
public class Message implements Comparable < Message > {

    private String message;
    private int mid;
    private int fromDevice;
    private int sequence;
    private int suggestedBy;
    private boolean deliverable;

    //The constructor

    public Message(String message, int mid, int fromDevice, int propsedNumber, int suggestedBy, boolean deliverable) {
        this.message = message;
        this.mid = mid;
        this.fromDevice = fromDevice;
        this.sequence = propsedNumber;
        this.suggestedBy = suggestedBy;
        this.deliverable = deliverable;
    }


    public Message(String message) {
        this.message = message;
    }


    //Returns the sequence
    public int getKey() {
        return this.sequence;
    }
    public int getMid() {
        return this.mid;
    }
    public boolean setKey(int key) {
        this.sequence = key;
        return true;
    }
    public int getFromDevice() {
        return this.fromDevice;
    }
    public void setDeliverable() {
        this.deliverable = true;
    }
    public boolean getStatus() {
        return this.deliverable;
    }
    public void setSuggestedBy(int suggestedBy) {
        this.suggestedBy = suggestedBy;
    }

    public String getMessage() {
        return this.message;
    }
    public int getOrigin() {
        return this.fromDevice;
    }

    public String getData() {

        return this.message + "::" + this.mid + "::" + this.fromDevice + "::" + this.sequence + "::" + this.suggestedBy + "::" + this.deliverable;
    }

    //Compare using the sequences
    // If tie, then compare by the process that suggested the sequence number
    //Even if that is a tie, compare using the delivery status
    @Override
    public int compareTo(Message another) {
        if (this.sequence == another.sequence) {

            if (this.getStatus() == another.getStatus()) {
                return Integer.compare(this.suggestedBy, another.suggestedBy);
            } else {
                return Boolean.compare(this.getStatus(), another.getStatus());
            }
        } else {
            return Integer.compare(this.sequence, another.sequence);
        }
    }
}