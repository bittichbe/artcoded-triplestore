package tech.artcoded.triplestore.sparql;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.vocabulary.RDF;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
public class SparqlClient {

  private static final String QUERY_FETCH_SUBJECT_URI = "SELECT ?s ?p ?o where {BIND(<${subject}> as ?s) ?s ?p ?o}";

  @Value("${sparql.endpoint.read}")
  private String readEndpoint;
  @Value("${sparql.endpoint.update}")
  private String updateEndpoint;


  public void insertModel(String graphUri, Model model) {
    var triples = ModelUtils.toString(model, RDFLanguages.NTRIPLES);
    String updateQuery = String.format("INSERT DATA { GRAPH <%s> { %s } }", graphUri, triples);
    executeUpdateQuery(updateQuery);
  }

  @SneakyThrows
  public void executeUpdateQuery(String updateQuery) {
    log.debug(updateQuery);
    try (var httpClient = buildHttpClient(); RDFConnection conn = RDFConnectionRemote.create()
                                                                                     .destination(updateEndpoint)
                                                                                     .httpClient(httpClient)
                                                                                     .build()) {
      conn.update(updateQuery);
    }

  }

  @SneakyThrows
  public <R> R executeSelectQuery(String query, Function<ResultSet, R> resultHandler) {
    log.debug(query);
    try (var httpClient = buildHttpClient();
         QueryExecution queryExecution = QueryExecutionFactory.sparqlService(readEndpoint, query, httpClient)) {
      return resultHandler.apply(queryExecution.execSelect());
    }
  }

  public List<Map<String, String>> executeSelectQueryAsListMap(String query) {
    var list = executeSelectQuery(query, resultSet -> {
      List<Map<String, String>> mapList = new ArrayList<>();
      List<String> resultVars = resultSet.getResultVars();
      resultSet.forEachRemaining(querySolution -> {
        Map<String, String> collect = resultVars.stream().map(v -> {
                                                  var node = querySolution.get(v);
                                                  if (node == null) {
                                                    return null;
                                                  }
                                                  if (node.isResource()) {
                                                    return Map.entry(v, node.asResource().getURI());
                                                  }
                                                  return Map.entry(v, node.asLiteral().getString());
                                                })
                                                .filter(Objects::nonNull)
                                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        mapList.add(collect);
      });
      return mapList;
    });
    return list;
  }

  public String executeSelectAsOutput(String query, Lang formatter) {
    return executeSelectQuery(query, resultSet -> {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      ResultSetFormatter.output(outputStream, resultSet, formatter);
      return outputStream.toString();

    });


  }

  public Model executeSelectQuery(String query) {
    return executeSelectQuery(query, resultSet -> {
      Model model = ModelFactory.createDefaultModel();
      resultSet.forEachRemaining(querySolution -> {
        RDFNode subject = querySolution.get("s");
        RDFNode predicate = querySolution.get("p");
        RDFNode object = querySolution.get("o");
        var triple = Triple.create(subject.asNode(), predicate.asNode(), object.asNode());
        model.getGraph().add(triple);
      });
      return model;
    });
  }

  @SneakyThrows
  public boolean executeAskQuery(String askQuery) {
    log.debug(askQuery);
    try (var httpClient = buildHttpClient();
         QueryExecution queryExecution = QueryExecutionFactory.sparqlService(readEndpoint, askQuery, httpClient)) {
      return queryExecution.execAsk();
    }
  }

  public void dropGraph(String graphUri) {
    executeUpdateQuery("clear graph <%s>".formatted(graphUri));
  }


  private Model fetchSubject(String uri, List<String> uriProcessed) {
    if (StringUtils.isEmpty(uri) || uriProcessed.contains(uri)) {
      return ModelFactory.createDefaultModel();
    }
    String query = SparqlQueryStore.computeQueryWithParameters(QUERY_FETCH_SUBJECT_URI, Map.of("subject", uri));
    Model modelFromUri = this.executeSelectQuery(query);
    uriProcessed.add(uri);
    Model model = modelFromUri.listStatements()
                              .filterDrop(stmt -> stmt.getPredicate().equals(RDF.type) || !stmt.getObject().isURIResource())
                              .toList()
                              .stream()
                              .sequential()
                              .map(stmt -> stmt.getObject().asResource().getURI())
                              .peek(iri -> log.debug("uri {}", iri))
                              .map(iri -> this.fetchSubject(iri, uriProcessed))
                              .reduce(Model::add)
                              .orElseGet(ModelFactory::createDefaultModel);
    return ModelFactory.createUnion(modelFromUri, model);
  }

  /**
   * fetch all the triples linked to a subject uri
   *
   * @param subjectUri the subject uri
   * @return
   */
  public Model fetchTriplesLinkedToSubject(String subjectUri) {
    return fetchSubject(subjectUri, new ArrayList<>());
  }

  public CloseableHttpClient buildHttpClient() {
    return HttpClients.custom()
                      .build();

  }


  @SneakyThrows
  public SparqlResult executeQuery(Query q, String acceptHeader) {
    try (var httpClient = buildHttpClient();
         QueryExecution queryExecution = QueryExecutionFactory.sparqlService(readEndpoint, q, httpClient)) {
      Lang lang = ModelUtils.contentTypeToLang(acceptHeader);

      return switch (q.queryType()) {
        case ASK -> ModelUtils.tryFormat(queryExecution.execAsk(), lang);
        case SELECT -> ModelUtils.tryFormat(queryExecution.execSelect(), lang);
        case DESCRIBE -> SparqlResult.builder()
                                     .body(ModelUtils.toString(queryExecution.execDescribe(), lang))
                                     .contentType(lang.getContentType().getContentTypeStr())
                                     .build();
        case CONSTRUCT -> SparqlResult.builder()
                                      .body(ModelUtils.toString(queryExecution.execConstruct(), lang))
                                      .contentType(lang.getContentType().getContentTypeStr())
                                      .build();
        default -> throw new UnsupportedOperationException(q.queryType() + " Not supported");
      };
    }
  }
}
