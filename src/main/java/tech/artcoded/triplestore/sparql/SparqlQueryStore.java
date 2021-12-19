package tech.artcoded.triplestore.sparql;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateMethodModelEx;
import lombok.SneakyThrows;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import static org.springframework.ui.freemarker.FreeMarkerTemplateUtils.processTemplateIntoString;

public interface SparqlQueryStore {
  Map<String, String> asMap();

  default String getQuery(String queryName) {
    return asMap().get(queryName);
  }

  default long size() {
    return asMap().size();
  }

  default boolean isPresent(String queryName) {
    return asMap().containsKey(queryName);
  }

  @SneakyThrows
  default String getQueryWithParameters(String queryName, Map<String, Object> parameters) {
    String query = getQuery(queryName);
    return computeQueryWithParameters(query, parameters);
  }

  static String computeQuery(String query, Object... parameters) {
    return query.formatted(parameters);
  }

  @SneakyThrows
  static String computeQueryWithParameters(String query, Map<String, Object> parameters) {
    Configuration cfg = new Configuration(Configuration.VERSION_2_3_31);
    Template template = new Template("name", new StringReader(query), cfg);
    Map<String, Object> params = new HashMap<>(parameters);
    params.put("_uuid", (TemplateMethodModelEx) (list) -> ModelUtils.uuid());
    return processTemplateIntoString(template, params);
  }
}
