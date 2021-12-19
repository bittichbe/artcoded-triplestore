package tech.artcoded.triplestore.sparql;

import org.apache.camel.Body;
import org.apache.camel.Header;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

import static tech.artcoded.triplestore.sparql.QueryParserUtil.parseQuery;


@Component
public class ReadRouteBuilder extends RouteBuilder {
  private final SparqlClient sparqlClient;

  public ReadRouteBuilder(SparqlClient sparqlClient) {
    this.sparqlClient = sparqlClient;
  }

  @Override
  public void configure() throws Exception {
    from("jms:queue:sparql-read")
            .routeId("ReadRoute::EntryPoint")
            .log("receiving query:\n ${body}")
            .transform().method(this, "process")
            .log("receiving response:\n ${body}")
    ;
  }

  public SparqlResult process(@Body String query, @Header("accept") String acceptHeader) {
    //return sparqlClient.executeQuery(parseQuery(query), acceptHeader);
    return null;
  }
}
