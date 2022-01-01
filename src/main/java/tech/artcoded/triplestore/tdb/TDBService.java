package tech.artcoded.triplestore.tdb;

import com.google.common.collect.Lists;
import com.google.common.io.FileBackedOutputStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.atlas.web.ContentType;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionDatasetBuilder;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.system.Txn;
import org.apache.jena.update.UpdateExecution;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import tech.artcoded.triplestore.sparql.QueryParserUtil;
import tech.artcoded.triplestore.sparql.SparqlResult;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.jena.query.ResultSetFormatter.output;
import static org.apache.jena.riot.Lang.TURTLE;
import static org.apache.jena.riot.RDFDataMgr.write;
import static org.apache.jena.riot.resultset.ResultSetLang.RS_CSV;
import static org.apache.jena.riot.resultset.ResultSetLang.RS_JSON;
import static org.apache.jena.riot.resultset.ResultSetLang.RS_Text;
import static org.apache.jena.riot.resultset.ResultSetLang.RS_XML;

@Service
@Slf4j
public class TDBService {

  private static final int THRESHOLD = 4 * 1024 * 1024;  // 4mb

  private final Dataset ds;

  @Value("${triplestore.batchSize}")
  private int batchSize;
  @Value("${triplestore.maxRetry}")
  private int maxRetry;
  @Value("${triplestore.query.timeout}")
  private long timeout;

  public TDBService(Dataset ds) {
    this.ds = ds;
  }

  public SparqlResult executeQuery(Query q, String acceptHeader) {
    Supplier<SparqlResult> _executeQuery = () -> {
      try (QueryExecution queryExecution = QueryExecutionDatasetBuilder.create()
                                                                       .query(q)
                                                                       .dataset(ds)
                                                                       .timeout(timeout, TimeUnit.SECONDS)
                                                                       .build()
      ) {
        return switch (q.queryType()) {
          case ASK -> tryFormat((lang, out) -> output(out, queryExecution.execAsk(), lang), acceptHeader, RS_JSON);
          case SELECT -> tryFormat((lang, out) -> output(out, queryExecution.execSelect(), lang), acceptHeader, RS_JSON);
          case DESCRIBE -> tryFormat((lang, out) -> write(out, queryExecution.execDescribe(), lang), acceptHeader, TURTLE);
          case CONSTRUCT -> tryFormat((lang, out) -> write(out, queryExecution.execConstruct(), lang), acceptHeader, TURTLE);
          default -> throw new UnsupportedOperationException(q.queryType() + " Not supported");
        };
      }
      catch (Exception exc) {
        log.error("exception occurred", exc);
        throw new RuntimeException(exc);
      }
    };
    return this.executeQueryTimeout(() -> Txn.calculateRead(ds, _executeQuery));
  }

  private SparqlResult executeQueryTimeout(Supplier<SparqlResult> supplier) {
    CompletableFuture<SparqlResult> future = CompletableFuture.supplyAsync(supplier);
    try {
      return future.get(timeout, TimeUnit.SECONDS);
    }
    catch (TimeoutException | InterruptedException | ExecutionException e) {
      future.cancel(true);
      throw new RuntimeException(e);
    }
  }

  private SparqlResult tryFormat(BiConsumer<Lang, OutputStream> consumer, String contentType, Lang fallback) {
    Lang lang = guessLang(contentType, fallback);
    var body = writeToOutputStream(outputStream -> consumer.accept(lang, outputStream));

    return SparqlResult.builder()
                       .contentType(lang.getContentType().getContentTypeStr())
                       .body(body)
                       .build();
  }

  @SneakyThrows
  private InputStream writeToOutputStream(Consumer<OutputStream> consumer) {
    try (var outputStream = new FileBackedOutputStream(THRESHOLD, true)) {
      consumer.accept(outputStream);
      return outputStream.asByteSource().openStream();
    }
  }

  private Lang guessLang(String contentType, Lang fallback) {
    try {
      return Stream.concat(RDFLanguages.getRegisteredLanguages().stream(), Stream.of(RS_Text, RS_JSON, RS_XML, RS_CSV))
                   .filter(l -> l.getContentType().equals(ContentType.create(contentType)))
                   .findFirst().orElse(fallback);
    }
    catch (Exception exc) {
      log.error("unexpected exception occurred", exc);
      return fallback;
    }

  }

  @SneakyThrows
  public void executeUpdateQuery(String updateQuery) {
    Txn.executeWrite(ds, () -> QueryParserUtil.parseUpdate(updateQuery)
                                              .map(u -> u.query() instanceof UpdateRequest updates ? updates : null)
                                              .map(u -> UpdateExecutionFactory.create(u, ds))
                                              .ifPresent(UpdateExecution::execute));

  }

  public void insertModel(String graphUri, Model model) {
    var triples = writeToOutputStream(outputStream -> RDFDataMgr.write(outputStream, model, RDFFormat.NTRIPLES));
    String updateQuery = String.format("INSERT DATA { GRAPH <%s> { %s } }", graphUri, triples);
    executeUpdateQuery(updateQuery);
  }

  public void batchLoadData(String graph, Model model) {
    log.info("running import triples with batch size {}, model size: {}, graph: <{}>", batchSize, model.size(), graph);
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
         .peek(batchModel -> log.info("running import triples with model size {}", batchModel.size()))
         .forEach(batchModel -> this.insertModelOrRetry(graph, batchModel));
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
