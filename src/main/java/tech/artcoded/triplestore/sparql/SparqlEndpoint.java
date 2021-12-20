package tech.artcoded.triplestore.sparql;

import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.query.Query;
import org.apache.jena.update.UpdateRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import tech.artcoded.triplestore.tdb.TDBService;

import javax.servlet.http.HttpServletRequest;

import static org.springframework.http.HttpHeaders.ACCEPT;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;


@Controller()
@RequestMapping("/sparql")
@CrossOrigin("*")
public class SparqlEndpoint {
  private final ProducerTemplate producerTemplate;
  private final TDBService tdbService;

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

    if (StringUtils.isNotEmpty(query)) {
      return tryParseExecute(query, accept);
    }
    else {
      return tryParseExecute(update, accept);
    }
  }

  ResponseEntity<String> tryParseExecute(String query, String accept) {
    if (StringUtils.isNotEmpty(query)) {
      var operation = QueryParserUtil.parseOperation(query);
      if (operation instanceof Query) {
        return executeRead(query, accept);
      }
      else if (operation instanceof UpdateRequest) {
        return executeUpdate(query, accept);
      }
    }
    return ResponseEntity.ok().build(); // just a ping
  }

  ResponseEntity<String> executeRead(String query, String accept) {
    this.producerTemplate.sendBodyAndHeader("jms:queue:sparql-read", ExchangePattern.InOnly, query,
                                            "accept", accept);
    var response = tdbService.executeQuery(query, accept);
    return ResponseEntity.status(200).header(CONTENT_TYPE, response.getContentType())
                         .body(response.getBody());
  }

  ResponseEntity<String> executeUpdate(String update, String accept) {
    this.producerTemplate.sendBody("jms:queue:sparql-update", ExchangePattern.InOnly, update);
    return ResponseEntity.status(200).header(CONTENT_TYPE, accept)
                         .body("{}");
  }
}
