package tech.artcoded.triplestore.sparql;

import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.atlas.web.ContentType;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.resultset.ResultsFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface ModelUtils {

  String DEFAULT_WELL_KNOWN_PREFIX = "http://bittich.be/.well-known/genid";

  Logger log = LoggerFactory.getLogger(ModelUtils.class);

  static Model toModel(String value, String lang) {
    if (StringUtils.isEmpty(value)) throw new RuntimeException("model cannot be empty");
    return toModel(IOUtils.toInputStream(value, StandardCharsets.UTF_8), lang);
  }

  static String uuid() {
    return StringUtils.substring(UUID.randomUUID().toString(), 0, 32);
  }

  static String formattedDate(LocalDateTime ldt) {
    return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
            ldt.atZone(ZoneId.systemDefault())
    );
  }

  static boolean equals(Model firstModel, Model secondModel) {
    return firstModel.isIsomorphicWith(secondModel);
  }

  static Model difference(Model firstModel, Model secondModel) {
    return firstModel.difference(secondModel);
  }

  static Model intersection(Model firstModel, Model secondModel) {
    return firstModel.intersection(secondModel);
  }

  static Model toModel(InputStream is, String lang) {
    try (var stream = is) {
      Model graph = ModelFactory.createDefaultModel();
      graph.read(stream, "", lang);
      return graph;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static Model toModel(InputStream is, Lang lang) {
    return toModel(is, lang.getName());
  }

  static Lang filenameToLang(String filename) {
    return RDFLanguages.filenameToLang(filename);
  }

  static Lang filenameToLang(String filename, Lang fallback) {
    return RDFLanguages.filenameToLang(filename, fallback);
  }

  static String getContentType(String lang) {
    return getContentType(getRdfLanguage(lang));
  }

  static String getContentType(Lang lang) {
    return lang.getContentType().getContentTypeStr();
  }

  static String getExtension(String lang) {
    return getExtension(getRdfLanguage(lang));
  }

  static String getExtension(Lang lang) {
    return lang.getFileExtensions().stream().findFirst().orElse("txt");
  }

  static Lang getRdfLanguage(String lang) {
    return RDFLanguages.nameToLang(lang);
  }

  static Lang contentTypeToLang(String contentType, Lang fallback) {
    try {
      Lang lang = RDFLanguages.contentTypeToLang(contentType);
      if (lang != null) {
        return lang;
      }
    }
    catch (Exception exc) {
      log.error("unknown content type, swicth to default", exc);
    }
    return fallback;
  }

  static String toString(Model model, Lang lang) {
    StringWriter writer = new StringWriter();
    model.write(writer, lang.getName());
    return writer.toString();
  }

  static String toString(Dataset dataset, Lang lang) {
    StringWriter writer = new StringWriter();
    RDFDataMgr.write(writer, dataset, lang);
    return writer.toString();
  }

  static byte[] toBytes(Model model, Lang lang) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    model.write(bos, lang.getName());
    return bos.toByteArray();
  }

  static Model replaceAnonNodes(Model model, String nodePrefix) {
    Model m = ModelFactory.createDefaultModel();
    model.listStatements().toList()
         .stream()
         .map(statement -> {
           var subject = statement.getSubject();
           var predicate = statement.getPredicate();
           var object = statement.getObject();
           if (subject.isAnon()) {
             subject = ResourceFactory.createResource(blankNodeToIriString(subject.asNode(), nodePrefix));
           }
           if (predicate.isAnon()) {
             predicate = ResourceFactory.createProperty(blankNodeToIriString(predicate.asNode(), nodePrefix));
           }
           if (object.isResource() && object.isAnon()) {
             object = ResourceFactory.createProperty(blankNodeToIriString(object.asNode(), nodePrefix));
           }
           return ResourceFactory.createStatement(subject, predicate, object);
         })
         .forEach(m::add);
    return m;
  }

  static Model replaceAnonNodes(Model model) {
    return replaceAnonNodes(model, DEFAULT_WELL_KNOWN_PREFIX);
  }

  static String blankNodeToIriString(Node node, String nodePrefix) {
    if (node.isBlank()) {
      String label = node.getBlankNodeLabel();
      return "%s/%s".formatted(nodePrefix, label);
    }
    if (node.isURI())
      return node.getURI();
    throw new RiotException("Not a blank node or URI");
  }


  @SneakyThrows
  static File toFile(Model content, Lang rdfLang, String path) {
    var file = new File(path);
    content.write(new FileWriter(file), rdfLang.getName());
    return file;
  }

  static Model merge(Model modelA, Model modelB) {
    return ModelFactory.createUnion(modelA, modelB);
  }

  private static void extractFromModel(Resource subject, Model model, Model newModel, List<String> statementsProcessed) {
    Model m = model.listStatements(subject, null, (RDFNode) null).toModel();
    newModel.add(m);
    m.listStatements().toList().stream()
     .filter(statement -> statement.getObject().isResource())
     .map(statement -> statement.getObject().asResource())
     .filter(resource -> !statementsProcessed.contains(resource.getURI()))
     .forEach(s -> extractFromModel(s, model, newModel, Stream.concat(statementsProcessed.stream(), Stream.of(s.getURI()))
                                                              .collect(Collectors.toList())));
  }

  /**
   * Extract all the triples linked to a subject from a model to a new model.
   * This method is very handy if you are just interested by a specific part of a graph
   *
   * @param subject
   * @param model
   * @param newModel
   */
  static void extractFromModel(Resource subject, Model model, Model newModel) {
    extractFromModel(subject, model, newModel, List.of());
  }

  static Model extractFromModel(Resource subject, Model model) {
    var newModel = ModelFactory.createDefaultModel();
    extractFromModel(subject, model, newModel, List.of());
    return newModel;
  }

  static SparqlResult tryFormat(ResultSet resultSet, String contentType) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    var ct = contentType;
    try {
      ResultSetFormatter.output(baos, resultSet, ResultsFormat.lookup(ct));
    }
    catch (Exception ex) {
      Lang rsJson = ResultSetLang.RS_JSON;
      log.error("could not format content type, fallback with lang {}", rsJson);
      ResultSetFormatter.outputAsJSON(baos, resultSet);
      ct = rsJson.getContentType().getContentTypeStr();
    }
    return SparqlResult.builder().contentType(ct)
                       .body(IOUtils.toString(baos.toByteArray(), StandardCharsets.UTF_8.name())).build();
  }

  static SparqlResult tryFormat(Boolean ask, String contentType) {
    var ct = contentType;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      ResultSetFormatter.output(baos, ask, RDFLanguages.contentTypeToLang(ContentType.create(ct)));
    }
    catch (Exception ex) {
      Lang rsJson = ResultSetLang.RS_JSON;
      log.error("could not format content type, fallback with lang {}", rsJson);

      ResultSetFormatter.outputAsJSON(baos, ask);
      ct = rsJson.getContentType().getContentTypeStr();
    }
    return SparqlResult.builder().contentType(ct)
                       .body(IOUtils.toString(baos.toByteArray(), StandardCharsets.UTF_8.name())).build();
  }

  static SparqlResult tryFormat(Model model, String contentType) {
    var ct = contentType;
    String body = null;
    try {
      body = ModelUtils.toString(model, RDFLanguages.contentTypeToLang(ContentType.create(ct)));
    }
    catch (Exception ex) {
      Lang ttl = Lang.TURTLE;
      log.error("could not format content type, fallback with lang {}", ttl);
      body = ModelUtils.toString(model, ttl);
      ct = ttl.getContentType().getContentTypeStr();
    }
    return SparqlResult.builder().contentType(ct)
                       .body(body).build();
  }
}
