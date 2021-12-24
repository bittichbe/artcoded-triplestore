package tech.artcoded.triplestore.sparql.route;

public interface Constants {

  String HEADER_TITLE = "NotificationTitle";
  String HEADER_TYPE = "NotificationType";
  String SYNC_FILE_TRIPLESTORE = "SYNC_FILE_TRIPLESTORE";
  String UPDATE_QUERY_TRIPLESTORE = "UPDATE_QUERY_TRIPLESTORE";
  String CORRELATION_ID = "CorrelationId";
  String NOTIFICATION_ENDPOINT = "jms:topic:notification";
}
