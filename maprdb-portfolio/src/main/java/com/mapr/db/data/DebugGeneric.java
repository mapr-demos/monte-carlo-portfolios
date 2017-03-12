package com.mapr.db.data;

import java.util.ArrayList;

/**
 * Created by pborne on 3/11/17.
 */
public class DebugGeneric<Type> {
    public void dump(ArrayList<Type> buffer) {
        for (int i = 0; i < buffer.size(); i++)
            System.out.print(buffer.get(i) + " ");
        System.out.println();
    }
}
