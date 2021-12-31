package tech.artcoded.triplestore.sparql;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.query.Query;
import org.apache.jena.update.UpdateRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import tech.artcoded.triplestore.tdb.TDBService;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.springframework.http.HttpHeaders.ACCEPT;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static tech.artcoded.triplestore.sparql.QueryParserUtil.parseOperation;


@RestController
@ConfigurationProperties("application.security.sparql.update")
@Slf4j
public class SparqlEndpoint {
  private final ProducerTemplate producerTemplate;
  private final TDBService tdbService;

  @Value("${application.security.enabled}")
  private boolean securityEnabled;

  @Setter
  private Set<String> allowedRoles;

  public SparqlEndpoint(ProducerTemplate producerTemplate, TDBService tdbService) {
    this.producerTemplate = producerTemplate;
    this.tdbService = tdbService;
  }

  @RequestMapping(value = "/public/sparql",
                  method = {RequestMethod.GET, RequestMethod.POST})
  public ResponseEntity<StreamingResponseBody> executePublicQuery(@RequestParam(value = "query",
                                                                                required = false) String query,
                                                                  @RequestParam(value = "update",
                                                                                required = false) String update,
                                                                  HttpServletRequest request) {
    return execute(query, update, request, true);
  }

  @RequestMapping(value = "/sparql",
                  method = {RequestMethod.GET, RequestMethod.POST})
  public ResponseEntity<StreamingResponseBody> executeQuery(@RequestParam(value = "query",
                                                                          required = false) String query,
                                                            @RequestParam(value = "update",
                                                                          required = false) String update,
                                                            HttpServletRequest request) {
    return execute(query, update, request, false);
  }

  ResponseEntity<StreamingResponseBody> execute(String query, String update, HttpServletRequest request, boolean forceRead) {
    String accept = request.getHeader(ACCEPT);

    return ofNullable(query).filter(StringUtils::isNotEmpty)
                            .or(() -> ofNullable(update))
                            .map(q -> tryParseExecute(q, accept, forceRead))
                            .orElseGet(ResponseEntity.noContent()::build);
  }


  ResponseEntity<StreamingResponseBody> tryParseExecute(String query, String accept, boolean forceRead) {
    try {
      return parseOperation(query, forceRead).flatMap(operation -> switch (operation.type()) {
        case READ:
          if (operation.query() instanceof Query q) yield of(executeRead(q, accept));
          else yield empty();
        case UPDATE:
          if (operation.query() instanceof UpdateRequest q) yield of(executeUpdate(q));
          else yield empty();
      }).orElseGet(() -> ResponseEntity.noContent().build());
    }
    catch (Exception exc) {
      return ResponseEntity.status(400).body((out) -> IOUtils.write("{error: '%s'}".formatted(exc.getMessage()), out, UTF_8));
    }
  }

  ResponseEntity<StreamingResponseBody> executeRead(Query query, String accept) {
    runAsync(() -> this.producerTemplate.sendBodyAndHeader("jms:queue:sparql-read", ExchangePattern.InOnly, query.serialize(),
                                                           "accept", accept));
    var response = tdbService.executeQuery(query, accept);
    return ResponseEntity.status(200).header(CONTENT_TYPE, response.getContentType())
                         .body((out) -> {
                           try (var is = response.getBody()) {
                             IOUtils.copyLarge(is, out);
                           }
                         });
  }

  ResponseEntity<StreamingResponseBody> executeUpdate(UpdateRequest update) {
    if (!canUpdate()) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN)
                           .body(out -> IOUtils.write("You cannot perform this action", out, UTF_8));
    }
    CompletableFuture.runAsync(() -> this.producerTemplate.sendBody("jms:queue:sparql-update", ExchangePattern.InOnly, update.toString()));
    return ResponseEntity.status(200)
                         .body((out) -> IOUtils.write("processing update", out, UTF_8));
  }

  boolean canUpdate() {
    if (securityEnabled) {
      List<String> roles = ofNullable(allowedRoles).orElseGet(Set::of)
                                                   .stream()
                                                   .map("ROLE_"::concat)
                                                   .peek(log::debug)
                                                   .toList();
      Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
      return ofNullable(authentication)
              .stream()
              .map(Authentication::getAuthorities)
              .flatMap(a -> a.stream().map(GrantedAuthority::getAuthority))
              .peek(log::debug)
              .anyMatch(roles::contains);

    }
    return true;
  }

}
