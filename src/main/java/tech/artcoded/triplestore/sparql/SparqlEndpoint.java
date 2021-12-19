package tech.artcoded.triplestore.sparql;

import org.apache.camel.ProducerTemplate;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


import static org.springframework.http.HttpHeaders.ACCEPT;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;


@RestController()
@RequestMapping("/sparql")
@CrossOrigin("*")
public class SparqlEndpoint {
  private final ProducerTemplate producerTemplate;

  public SparqlEndpoint(ProducerTemplate producerTemplate) {
    this.producerTemplate = producerTemplate;
  }

  @RequestMapping(value = "",
                  method = {RequestMethod.GET, RequestMethod.POST})
  public ResponseEntity<String> executeQuery(@RequestParam(value = "query",
                                                           required = false) String query,
                                             @RequestParam(value = "update",
                                                           required = false) String update,
                                             ServerHttpRequest request) {
    String accept = request.getHeaders().get(ACCEPT).get(0);
    if (StringUtils.isNotEmpty(query)) {
      var response = this.producerTemplate.requestBodyAndHeader("jms:queue:sparql-read", query,
                                                                "accept", accept,
                                                                SparqlResult.class);
      return ResponseEntity.status(200).header(CONTENT_TYPE, response.getContentType())
                           .body(response.getBody());
    }
    if (StringUtils.isNotEmpty(update)) {
      this.producerTemplate.sendBody("jms:queue:sparql-update", update);
      return ResponseEntity.noContent().build();
    }
    return ResponseEntity.badRequest().build();
  }
}
