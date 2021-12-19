package tech.artcoded.triplestore.sparql;

import org.apache.camel.Body;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class UpdateRouteBuilder extends RouteBuilder {
  private final SparqlClient sparqlClient;

  public UpdateRouteBuilder(SparqlClient sparqlClient) {
    this.sparqlClient = sparqlClient;
  }

  @Override
  public void configure() throws Exception {
    from("jms:queue:sparql-update")
        .routeId("UpdateRoute::EntryPoint")
        .log("receiving update query:\n ${body}")
        .bean(() -> this, "process")
        .log("update done");
  }

  public void process(@Body String query) {
    // sparqlClient.executeUpdateQuery(query);
  }
}
