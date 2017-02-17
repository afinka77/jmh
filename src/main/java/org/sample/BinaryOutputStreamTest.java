package org.sample;


import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryOffheapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by admin on 16.02.2017.
 */
public class BinaryOutputStreamTest {

    private final String messageEN = "TestTestTest";
    private final String messageRU = "ТестТестТест";

    private final Charset UTF_8 = Charset.forName("UTF-8");

    private BinaryOutputStream outBinaryHeap;
    private BinaryOutputStream outBinaryOffheap;

    @Before
    public void prepare() {
        outBinaryHeap    = new BinaryHeapOutputStream(256);
        outBinaryOffheap = new BinaryOffheapOutputStream(256);
    }

    @Test
    public void testBinaryHeapOutputStream() {
        byte[] strArr = messageEN.getBytes(UTF_8);
        outBinaryHeap.writeByteArray(strArr);
    }


    @Test
    public void testBinaryOffheapOutputStream() {
        byte[] strArr = messageEN.getBytes(UTF_8);
        outBinaryOffheap.writeByteArray(strArr);
    }


    @After
    public void tearDown(){
        outBinaryHeap.close();
        outBinaryOffheap.close();
    }

    @Test
    public void testBinaryHeapOutputStream1() throws UnsupportedEncodingException {
        byte[] resultString2ByteEN = string2byte1(messageEN);
        String returnedMessageEN = new String(resultString2ByteEN);
        outBinaryHeap.writeByteArray(resultString2ByteEN);
        assertThat( returnedMessageEN, is(messageEN));
        byte[] resultString2ByteRU = string2byte1(messageRU);
        String returnedMessageRU = new String(resultString2ByteRU,"UTF-8");
        assertThat( returnedMessageRU, is(messageRU));
    }

    @Test
    public void testBinaryOffheapOutputStream1() throws UnsupportedEncodingException{
        byte[] resultString2Byte1 = string2byte1(messageEN);
        String returnedMessage = new String(resultString2Byte1);
        assertThat( returnedMessage, is(messageEN));
        outBinaryOffheap.writeByteArray(resultString2Byte1);
    }


    @Test
    public void test2() throws UnsupportedEncodingException {
        byte[] resultString2Byte2 = messageEN.getBytes("utf-8");
        String returnedMessage = new String(resultString2Byte2);
        assertThat( returnedMessage, is(messageEN));
        byte[] resultString2ByteRU = messageRU.getBytes("utf-8");
        String returnedMessageRU = new String(resultString2ByteRU);
        assertThat( returnedMessageRU, is(messageRU));
    }

    @Test
    public void test3() throws UnsupportedEncodingException{
        byte[] resultString2Byte3 = string2byte3(messageEN);
        String returnedMessage = new String(resultString2Byte3);
        assertThat( returnedMessage.trim(), is( messageEN));       
        byte[] resultString2ByteRU = string2byte3(messageRU);
        String returnedMessageRU = new String(resultString2ByteRU,"UTF-8");
        assertThat( returnedMessageRU.trim(), is(messageRU));
    }

    @Test
    public void test4() {
        byte[] resultString2Byte4 = string2byte4(messageEN);
        String returnedMessage = new String(resultString2Byte4);
        assertThat( returnedMessage, is( messageEN));
        byte[] resultString2ByteRU = string2byte4(messageRU);
        String returnedMessageRU = new String(resultString2ByteRU);
        assertThat( returnedMessageRU, is(messageRU));
    }

    @Test
    public void test5() {
        byte[] resultString2Byte5 = string2byte5(messageEN);
        String returnedMessage = new String(resultString2Byte5);
        assertThat( returnedMessage, is( messageEN));
        byte[] resultString2ByteRU = string2byte5(messageRU);
        String returnedMessageRU = new String(resultString2ByteRU);
        assertThat( returnedMessageRU, is(messageRU));
    }

    @Test
    public void testUTFCustom() {
        byte[] resultString2ByteUTFNIO = string2ByteUTFCustom(messageEN);
        String returnedMessage = byte2StringUTFCustom(resultString2ByteUTFNIO);
        assertThat( returnedMessage, is( messageEN));
        byte[] resultString2ByteUTFNIORU = string2ByteUTFCustom(messageRU);
        String returnedMessageRU = byte2StringUTFCustom(resultString2ByteUTFNIORU);
        assertThat( returnedMessageRU, is(messageRU));
    }

    @Test
    public void testASCII() throws UnsupportedEncodingException{
        byte[] resultString2ByteASCII = string2ByteASCII(messageEN);
        String returnedMessage = new String(resultString2ByteASCII);
        assertThat( returnedMessage, is( messageEN));
        byte[] resultString2ByteASCIIRU = string2ByteASCII(messageRU);
        String returnedMessageRU = new String(resultString2ByteASCIIRU,"UTF-8");
        assertThat( returnedMessageRU, is(messageRU));
    }

    @Test
    public void string2ByteUTFNIO() {
        byte[] resultString2ByteUTFNIO = string2ByteUTFNIO(messageEN);
        String returnedMessage = byte2StringUTFNIO(resultString2ByteUTFNIO);
        assertThat( returnedMessage, is("TestTestTest"));
        byte[] resultString2ByteUTFNIORU = string2ByteUTFNIO(messageRU);
        String returnedMessageRU = byte2StringUTFNIO(resultString2ByteUTFNIORU);
        assertThat( returnedMessageRU, is(messageRU));
    }

    public static byte[] string2byte1(String message) throws UnsupportedEncodingException {
    	//char[] chMessage1 = message.toCharArray(); 
    	byte[] chMessage1 =message.getBytes("UTF-8");
        byte[] btMessage1 = new byte[message.getBytes("UTF-8").length];
        for (int i = 0; i < chMessage1.length; i++) {
            btMessage1[i] = (byte) chMessage1[i];
        }
        return btMessage1;
    }

    public static byte[] string2byte3(String message) {
        char[] chMessage3 = message.toCharArray();
        CharBuffer chbMessage3 = CharBuffer.wrap(chMessage3);
        //ByteBuffer btbMessage3 = StandardCharsets.ISO_8859_1.encode(chbMessage3);
        ByteBuffer btbMessage3 = StandardCharsets.UTF_8.encode(chbMessage3);   
        byte[] btMessage3 = btbMessage3.array();
        return btMessage3;
    }

    private static byte[] string2byte4(String message) {
        char[] chMessage = message.toCharArray();
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        OutputStreamWriter wOut = new OutputStreamWriter(bOut, Charset.forName("UTF-8").newEncoder());
        PrintWriter pw = new PrintWriter(bOut);
        pw.write(chMessage);
        pw.close();
        byte[] bt = bOut.toByteArray();
        return bt;
    }

    public static byte[] string2byte5(String message) {
        char[] payLoad = message.toCharArray();
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(bOut);
        writer.print(payLoad);
        writer.close();
        return bOut.toByteArray();
    }

    private static byte[] string2ByteASCII(String message) throws UnsupportedEncodingException{
        byte[] a = message.getBytes("UTF-8");
        byte[] b = new byte[a.length];   	
        for (int i = 0; i < a.length; i++) {
            //b[i] = (byte) message.charAt(i);
        	b[i] = a[i];
        }
        return b;
    }

    private static String byte2StringUTFCustom(byte[] btMessage6) {
        char[] buffer = new char[btMessage6.length >> 1];
        for (int i = 0; i < buffer.length; i++) {
            int bpos = i << 1;
            char c = (char) (((btMessage6[bpos] & 0x00FF) << 8) + (btMessage6[bpos + 1] & 0x00FF));
            buffer[i] = c;
        }
        return new String(buffer);
    }

    private static byte[] string2ByteUTFCustom(String message) {
        char[] buffer = message.toCharArray();
        byte[] b = new byte[buffer.length << 1];
        for (int i = 0; i < buffer.length; i++) {
            int bpos = i << 1;
            b[bpos] = (byte) ((buffer[i] & 0xFF00) >> 8);
            b[bpos + 1] = (byte) (buffer[i] & 0x00FF);

        }
        return b;
    }

    public static byte[] string2ByteUTFNIO(String message) {
        char[] chars = message.toCharArray();
        byte[] bytes = new byte[chars.length << 1];
        CharBuffer charBuffer = ByteBuffer.wrap(bytes).asCharBuffer();
        for (int i = 0; i < chars.length; i++) {
            charBuffer.put(chars[i]);
        }
        return bytes;
    }

    public static String byte2StringUTFNIO(byte[] bytes) {
        CharBuffer cBuffer = ByteBuffer.wrap(bytes).asCharBuffer();
        return cBuffer.toString();
    }

}