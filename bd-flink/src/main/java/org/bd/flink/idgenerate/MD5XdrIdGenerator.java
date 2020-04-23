package org.bd.flink.idgenerate;

import lombok.Builder;
import lombok.NonNull;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Builder
public class MD5XdrIdGenerator implements ESIdGenerator {
    @Override
    public String generate(final String msg, final int idLength) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(msg.getBytes(), 0, msg.length());

            byte messageDigest[] = md.digest();

            char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
            char[] resultCharArray = new char[messageDigest.length * 2];
            int index = 0;
            for (byte b : messageDigest) {
                resultCharArray[index++] = hexDigits[b >>> 4 & 0xf];
                resultCharArray[index++] = hexDigits[b & 0xf];
            }
            String result = new String(resultCharArray);
            int startIndex = 0;
            int endIndex = 32;
            if(idLength <= 24 && idLength > 0){
                startIndex = 8;
                endIndex = 8 + idLength;
            }else if(idLength > 24 && idLength <=32){
                endIndex = idLength;
            }
            return result.substring(startIndex, endIndex);

        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }
}
