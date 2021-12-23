package tech.artcoded.triplestore.tdb;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import tech.artcoded.triplestore.sparql.ModelUtils;
import tech.artcoded.triplestore.sparql.QueryParserUtil;
import tech.artcoded.triplestore.sparql.SparqlResult;

import java.util.List;

import static tech.artcoded.triplestore.sparql.QueryParserUtil.parseQuery;

@Service
@Slf4j
public class TDBService {
  private final Dataset ds;

  @Value("${triplestore.batchSize}")
  private int batchSize;
  @Value("${triplestore.maxRetry}")
  private int maxRetry;

  public TDBService(Dataset ds) {
    this.ds = ds;
  }

  public SparqlResult executeQuery(String query, String acceptHeader) {
    ds.begin(ReadWrite.READ);
    try {
      var q = parseQuery(query);
      QueryExecution queryExecution = QueryExecutionFactory
              .create(q, ds);
      var result = switch (q.queryType()) {
        case ASK -> ModelUtils.tryFormat(queryExecution.execAsk(), acceptHeader);
        case SELECT -> ModelUtils.tryFormat(queryExecution.execSelect(), acceptHeader);
        case DESCRIBE -> ModelUtils.tryFormat(queryExecution.execDescribe(), acceptHeader);
        case CONSTRUCT -> ModelUtils.tryFormat(queryExecution.execConstruct(), acceptHeader);

        default -> throw new UnsupportedOperationException(q.queryType() + " Not supported");
      };
      ds.end();
      return result;
    }
    catch (Exception exc) {
      ds.abort();
      throw exc;
    }

  }

  @SneakyThrows
  public void executeUpdateQuery(String updateQuery) {
    ds.begin(ReadWrite.WRITE);
    try {
      UpdateRequest updates = QueryParserUtil.parseUpdate(updateQuery);
      UpdateProcessor updateProcessor =
              UpdateExecutionFactory.create(updates, ds);
      updateProcessor.execute();
      ds.commit();
      ds.end();
    }
    catch (Exception exc) {
      ds.abort();
      throw exc;
    }

  }
  public void insertModel(String graphUri, Model model) {
    var triples = ModelUtils.toString(model, RDFLanguages.NTRIPLES);
    String updateQuery = String.format("INSERT DATA { GRAPH <%s> { %s } }", graphUri, triples);
    executeUpdateQuery(updateQuery);
  }

  public void batchLoadData(String graph, Model model) {
    log.debug("running import triples with batch size {}, model size: {}, graph: <{}>", batchSize, model.size(), graph);
    List<Triple> triples = model.getGraph().find().toList(); //duplicate so we can splice
    Lists.partition(triples, batchSize)
         .stream()
         .parallel()
         .map(batch -> {
           Model batchModel = ModelFactory.createDefaultModel();
           Graph batchGraph = batchModel.getGraph();
           batch.forEach(batchGraph::add);
           return batchModel;
         })
         .forEach(batchModel -> this.insertModelOrRetry (graph, batchModel));
  }

  private void insertModelOrRetry(String graph, Model batchModel) {
    int retryCount = 0;
    boolean success = false;
    do {
      try {
        this.insertModel(graph, batchModel);
        success = true;
        break;
      }
      catch (Exception e) {
        log.error("an error occurred, retry count {}, max retry {}, error: {}", retryCount, maxRetry, e);
        retryCount += 1;
      }
    } while (retryCount < maxRetry);
    if (!success) {
      throw new RuntimeException("Reaching max retries. Check the logs for further details.");
    }
  }
}
