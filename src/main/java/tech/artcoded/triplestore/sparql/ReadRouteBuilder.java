package tech.artcoded.triplestore.sparql;

import org.apache.camel.Body;
import org.apache.camel.Header;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;
import tech.artcoded.triplestore.tdb.TDBService;

import static tech.artcoded.triplestore.sparql.QueryParserUtil.parseQuery;


@Component
public class ReadRouteBuilder extends RouteBuilder {
  private final TDBService sparqlClient;

  public ReadRouteBuilder(TDBService sparqlClient) {
    this.sparqlClient = sparqlClient;
  }

  @Override
  public void configure() throws Exception {
    onException(Throwable.class)
            .handled(true)
            .maximumRedeliveries(0)
            .transform(exceptionMessage())
            .log("an error occured: ${body}");

    from("jms:queue:sparql-read")
            .routeId("ReadRoute::EntryPoint")
            .log("receiving query:\n ${body}")
            .transform().method(this, "process")
            .log("receiving response:\n ${body}")
    ;
  }

  public SparqlResult process(@Body String query, @Header("accept") String acceptHeader) {
    return sparqlClient.executeQuery(query, acceptHeader);
  }
}
