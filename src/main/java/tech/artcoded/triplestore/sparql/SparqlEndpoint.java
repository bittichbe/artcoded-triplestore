package tech.artcoded.triplestore.sparql;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.apache.jena.query.Query;
import org.apache.jena.update.UpdateRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import tech.artcoded.triplestore.tdb.TDBService;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Set;

import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.springframework.http.HttpHeaders.ACCEPT;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;


@Controller()
@RequestMapping("/sparql")
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

  @RequestMapping(value = "",
                  method = {RequestMethod.GET, RequestMethod.POST})
  public ResponseEntity<String> executeQuery(@RequestParam(value = "query",
                                                           required = false) String query,
                                             @RequestParam(value = "update",
                                                           required = false) String update,
                                             HttpServletRequest request) {
    String accept = request.getHeader(ACCEPT);

    if (isNotEmpty(query)) {
      return tryParseExecute(query, accept);
    }
    else {
      return tryParseExecute(update, accept);
    }
  }

  ResponseEntity<String> tryParseExecute(String query, String accept) {
    try {
      if (isNotEmpty(query)) {
        var operation = QueryParserUtil.parseOperation(query);
        if (operation instanceof Query) {
          return executeRead(query, accept);
        }
        else if (operation instanceof UpdateRequest) {
          if (!canUpdate()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("You cannot perform this action");
          }
          return executeUpdate(query);
        }
      }
      return ResponseEntity.ok().build(); // just a ping
    }
    catch (Exception exc) {
      return ResponseEntity.status(400).body("error: %s".formatted(exc.getMessage()));
    }
  }

  ResponseEntity<String> executeRead(String query, String accept) {
    this.producerTemplate.sendBodyAndHeader("jms:queue:sparql-read", ExchangePattern.InOnly, query,
                                            "accept", accept);
    var response = tdbService.executeQuery(query, accept);
    return ResponseEntity.status(200).header(CONTENT_TYPE, response.getContentType())
                         .body(response.getBody());
  }

  ResponseEntity<String> executeUpdate(String update) {

    this.producerTemplate.sendBody("jms:queue:sparql-update", ExchangePattern.InOnly, update);
    return ResponseEntity.status(200)
                         .body("processing update");
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
