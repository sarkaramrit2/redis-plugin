package io.openindex.solr.redis;

import org.apache.lucene.queries.function.ValueSource;

import org.apache.solr.common.util.NamedList;;
// import org.apache.solr.parser.ParseException;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.ValueSourceParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisValueSourceParser extends ValueSourceParser {
  
  private static final Logger log = LoggerFactory.getLogger(RedisValueSource.class);
  protected String host;

  public void init(NamedList args) {
    this.host = (String)args.get("host");

    log.info("TEST RedisValueSourceParser::init()");
  }


  @Override 
  public ValueSource parse(FunctionQParser fp) {
    // log.info("TEST: RedisValueSourceParser:parse(fp)");
    try {
      String idField = fp.parseArg();
      String redisKey = fp.parseArg();

      return new RedisValueSource(idField, redisKey);
    } catch(org.apache.solr.search.SyntaxError e){
      log.error("TEST Exception parsing args: ", e);
    }
    return null;
  }
}