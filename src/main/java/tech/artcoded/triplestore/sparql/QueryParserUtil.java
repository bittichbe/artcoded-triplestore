package tech.artcoded.triplestore.sparql;


import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface QueryParserUtil {
  Logger LOGGER = LoggerFactory.getLogger(QueryParserUtil.class);

  static Object parseOperation(String query) {
    try {
      return QueryFactory.create(query);
    }
    catch (QueryException exception) {
      try {
        return UpdateFactory.create(query);
      }
      catch (Exception e) {
        LOGGER.error("unsupported operation", e);
        throw new UnsupportedOperationException();
      }
    }
  }

  static Query parseQuery(String query) {
    return QueryFactory.create(query);
  }

  static UpdateRequest parseUpdate(String query) {
    return UpdateFactory.create(query);
  }
}
class QueryParserUtil {
  
}
