package com.vdian.bigdata.meta.lineage.antlr;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;

/**
 * @author: lhy
 * @description
 * @created: 2019-06-27  16:47
 **/


public class Console {

    public static void run(String expr) throws Exception {
        ANTLRStringStream in = new ANTLRStringStream(expr);
        ExprLexer lexer = new ExprLexer(in);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        ExprParser parser = new ExprParser(tokens);
        parser.prog();
    }
}

    
