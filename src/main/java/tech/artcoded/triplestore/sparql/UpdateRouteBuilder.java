package tech.artcoded.triplestore.sparql;

import org.apache.camel.Body;
import org.apache.camel.ExchangePattern;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;
import tech.artcoded.triplestore.tdb.TDBService;

@Component
public class UpdateRouteBuilder extends RouteBuilder {
  private final TDBService sparqlClient;

  public UpdateRouteBuilder(TDBService sparqlClient) {
    this.sparqlClient = sparqlClient;
  }

  @Override
  public void configure() throws Exception {
    onException(Throwable.class)
            .handled(true)
            .maximumRedeliveries(5)
            .transform(exceptionMessage())
            .log("an error occured: ${body}")
            .to(ExchangePattern.InOnly, "jms:queue:sparql-update-failure");

    from("jms:queue:sparql-update")
        .routeId("UpdateRoute::EntryPoint")
        .log("receiving update query:\n ${body}")
        .bean(() -> this, "process")
        .log("update done");
  }

  public void process(@Body String query) {
     sparqlClient.executeUpdateQuery(query);
  }
}
