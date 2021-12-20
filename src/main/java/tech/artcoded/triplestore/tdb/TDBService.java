package tech.artcoded.triplestore.tdb;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.util.QueryUtils;
import org.apache.jena.system.Txn;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateRequest;
import org.springframework.stereotype.Service;
import tech.artcoded.triplestore.sparql.ModelUtils;
import tech.artcoded.triplestore.sparql.QueryParserUtil;
import tech.artcoded.triplestore.sparql.SparqlResult;

import static tech.artcoded.triplestore.sparql.ModelUtils.contentTypeToLang;
import static tech.artcoded.triplestore.sparql.QueryParserUtil.parseQuery;

@Service
@Slf4j
public class TDBService {
  private final Dataset ds;

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
    } catch (Exception exc){
      ds.abort();
      throw exc;
    }

  }

  @SneakyThrows
  public void executeUpdateQuery(String updateQuery) {
    ds.begin(ReadWrite.WRITE);
    try{
      UpdateRequest updates = QueryParserUtil.parseUpdate(updateQuery);
      UpdateProcessor updateProcessor =
              UpdateExecutionFactory.create(updates, ds);
      updateProcessor.execute();
      ds.commit();
      ds.end();
    }catch (Exception exc){
      ds.abort();
      throw exc;
    }

  }

  public void loadData(String graphUri, Model model) {
    var triples = ModelUtils.toString(model, RDFLanguages.NTRIPLES);
    String updateQuery = String.format("INSERT DATA { GRAPH <%s> { %s } }", graphUri, triples);
    executeUpdateQuery(updateQuery);
  }
}
