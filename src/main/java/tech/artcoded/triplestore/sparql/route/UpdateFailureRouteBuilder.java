package tech.artcoded.triplestore.sparql.route;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.builder.RouteBuilder;
import org.apache.commons.io.FilenameUtils;
import org.springframework.stereotype.Component;

import static java.time.LocalDateTime.now;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

@Component
public class UpdateFailureRouteBuilder extends RouteBuilder {
  @Override
  public void configure() throws Exception {
    from("jms:queue:sparql-update-failure")
            .routeId("UpdateFailure::Entrypoint")
            .setHeader(Exchange.FILE_NAME, () -> FilenameUtils.normalize(now().format(ISO_LOCAL_DATE_TIME).concat(".sparql")))
            .to(ExchangePattern.InOnly, "file:{{sparql.update.failure.directory}}");
  }
}
