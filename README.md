# artcoded-triplestore

e.g using security:

```
  triplestore:
    image: nbittich/triplestore
    restart: always
    environment:
      DATA_DIR: /triplestore
      SECURITY_CONFIG: "classpath:config/security.yml"
      ARTEMIS_CONFIG: "classpath:config/artemis.yml"
      ARTEMIS_URL: tcp://artemis:61616
      ARTEMIS_USER: root
      ARTEMIS_PASSWORD: root
      MIGRATION_PATH: /migrations
      MIGRATION_DEFAULT_GRAPH: "https://bittich.be/application"
      SERVER_PORT: 80
      JWK_SET_URI: http://auth.somehost.org:8080/auth/realms/Artcoded/protocol/openid-connect/certs
    volumes:
      - ./data/tdb2:/triplestore
      - ./data/migrations:/migrations
    ports:
      - 8888:80
```
