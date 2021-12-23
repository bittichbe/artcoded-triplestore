package tech.artcoded.triplestore.tdb;

import lombok.extern.slf4j.Slf4j;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.tdb2.assembler.VocabTDB2;
import org.apache.jena.vocabulary.RDF;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;

@Configuration
@Slf4j
public class TDBConfig {
  @Value("${triplestore.database.dir}")
  private String tripleStoreDir;
  @Value("${triplestore.database.unionDefaultGraph}")
  private boolean unionDefaultGraph;

  @Bean(destroyMethod = "close")
  public Dataset database() {
    File dir = new File(tripleStoreDir);
    if (!dir.exists()) {
      log.info("creating directory {}: {}", tripleStoreDir, dir.mkdirs());
    }

    Model assemblerModel = ModelFactory.createDefaultModel();
    Resource dataset = assemblerModel.createResource(AnonId.create("dataset"));
    dataset.addProperty(RDF.type, VocabTDB2.tDatasetTDB);
    dataset.addProperty(VocabTDB2.pLocation, tripleStoreDir);
    dataset.addLiteral(VocabTDB2.pUnionDefaultGraph, unionDefaultGraph);

    return (Dataset) AssemblerUtils.build(assemblerModel, VocabTDB2.tDatasetTDB);

  }
}
