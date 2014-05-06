package org.riderzen.flume.sink;

import ch.qos.logback.decoder.Decoder;

/**
 * Created with IntelliJ IDEA.
 * User: zh
 * Date: 14-5-4
 * Time: 下午7:19
 * To change this template use File | Settings | File Templates.
 */
public class DecoderLocal extends Decoder {
    private static DecoderLocal decoder;
    public static Decoder getInstance(String pattern){
        if(null == decoder){
              decoder = new DecoderLocal();
        }
        decoder.setLayoutPattern(pattern);
        return decoder;
    }

}
