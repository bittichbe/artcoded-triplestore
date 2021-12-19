package tech.artcoded.triplestore.tdb;

import lombok.extern.slf4j.Slf4j;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.TDB2Factory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;

@Configuration
@Slf4j
public class TDBConfig {
  @Value("${triplestore.database.dir}")
  private String tripleStoreDir;

  @Bean(destroyMethod = "close")
  public Dataset database(){
    File dir = new File(tripleStoreDir);
    if(!dir.exists()){
      log.info("creating folder {}: {}", tripleStoreDir, dir.mkdirs());
    }
    return TDB2Factory.connectDataset(tripleStoreDir);
  }
}
