package tech.artcoded.triplestore.sparql.route;

import org.apache.camel.Body;
import org.apache.camel.ExchangePattern;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;
import tech.artcoded.triplestore.tdb.TDBService;

import java.util.UUID;

import static tech.artcoded.triplestore.sparql.route.Constants.CORRELATION_ID;
import static tech.artcoded.triplestore.sparql.route.Constants.HEADER_TITLE;
import static tech.artcoded.triplestore.sparql.route.Constants.HEADER_TYPE;
import static tech.artcoded.triplestore.sparql.route.Constants.NOTIFICATION_ENDPOINT;
import static tech.artcoded.triplestore.sparql.route.Constants.UPDATE_QUERY_TRIPLESTORE;

@Component
public class UpdateRouteBuilder extends RouteBuilder {
  private final TDBService sparqlClient;

  public UpdateRouteBuilder(TDBService sparqlClient) {
    this.sparqlClient = sparqlClient;
  }

  @Override
  public void configure() throws Exception {
    onException(Exception.class)
            .handled(true)
            .maximumRedeliveries(5)
            .transform(exceptionMessage())
            .log(LoggingLevel.ERROR, "an error occured: ${body}")
            .setBody(exchangeProperty("oldBody"))
            .choice().when(body().isNotNull())
              .to(ExchangePattern.InOnly, "jms:queue:sparql-update-failure")
            .otherwise()
              .log("old body was cleared")
            .endChoice();

    from("jms:queue:sparql-update")
            .routeId("UpdateRoute::EntryPoint")
            .setProperty("oldBody", body())
            .log(LoggingLevel.INFO, "receiving update query:\n${body}")
            .bean(() -> this, "process")
            .log(LoggingLevel.DEBUG, "update done")
            .setProperty(HEADER_TITLE, simple("Update query has been executed to the triplestore"))
            .setProperty(HEADER_TYPE, constant(UPDATE_QUERY_TRIPLESTORE))
            .transform().body(o -> UUID.randomUUID().toString())
            .setHeader(CORRELATION_ID, body())
            .setHeader(HEADER_TITLE, exchangeProperty(HEADER_TITLE))
            .setHeader(HEADER_TYPE, exchangeProperty(HEADER_TYPE))
            .removeProperty("oldBody")
            .to(ExchangePattern.InOnly, NOTIFICATION_ENDPOINT)
    ;
  }

  public void process(@Body String query) {
    sparqlClient.executeUpdateQuery(query);
  }
}
