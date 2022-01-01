package tech.artcoded.triplestore.sparql;


import org.apache.commons.lang3.StringUtils;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.update.UpdateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

public interface QueryParserUtil {
  Logger LOGGER = LoggerFactory.getLogger(QueryParserUtil.class);

  enum QueryType {UPDATE, READ}

  record QueryTuple(Object query, QueryType type) {
  }

  static Optional<QueryTuple> parseOperation(String query, boolean forceRead) {

    return ofNullable(query).filter(StringUtils::isNotEmpty)
                            .flatMap(q -> parseQuery(q, forceRead).or(() -> parseUpdate(q)));
  }

  static Optional<QueryTuple> parseQuery(String query, boolean forceRead) {
    try {
      return of(new QueryTuple(QueryFactory.create(query), QueryType.READ));
    }
    catch (QueryException exception) {
      LOGGER.error("unsupported operation:", exception);
      if (forceRead) {
        throw new RuntimeException(exception.getMessage());
      }
      return empty();
    }
  }

  static Optional<QueryTuple> parseUpdate(String query) {
    try {
      return of(new QueryTuple(UpdateFactory.create(query), QueryType.UPDATE));
    }
    catch (Exception exc) {
      LOGGER.error("unsupported operation:", exc);
      throw new RuntimeException(exc.getMessage());
    }
  }

}
