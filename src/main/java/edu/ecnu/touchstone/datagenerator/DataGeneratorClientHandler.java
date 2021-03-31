package edu.ecnu.touchstone.datagenerator;

import edu.ecnu.touchstone.run.Touchstone;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.log4j.Logger;

public class DataGeneratorClientHandler extends ChannelInboundHandlerAdapter {

    private Logger logger = null;

    public DataGeneratorClientHandler() {
        logger = Logger.getLogger(Touchstone.class);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        String response = (String) msg;
        logger.info("\n\t" + response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
