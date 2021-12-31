package tech.artcoded.triplestore.sparql.route;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;


@Component
public class ReadRouteBuilder extends RouteBuilder {

  @Override
  public void configure() throws Exception {
    onException(Throwable.class)
            .handled(true)
            .maximumRedeliveries(0)
            .transform(exceptionMessage())
            .log(LoggingLevel.DEBUG, "an error occured: ${body}");
    from("jms:queue:sparql-read")
            .routeId("ReadRoute::EntryPoint")
            .log(LoggingLevel.INFO, "receiving query:\n${body}");
    ;
  }
}
