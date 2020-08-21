package com;

import org.testng.annotations.Test;

public class TestMethod {
    private int param=0;

    @Test(dataProvider = "dataMethod", dataProviderClass = Data.class)
    public void testOne(){
        int op = param+1;
        System.out.println("Test One: " + op);
    }

    @Test(dataProvider = "dataMethod", dataProviderClass = Data.class)
    public void testTwo(){
        int op = param+2;
        System.out.println("Test Two: " + op);
    }
}
