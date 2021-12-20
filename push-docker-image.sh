set -e
docker build -t nbittich/triplestore .
docker tag nbittich/triplestore nbittich/triplestore
docker push nbittich/triplestore
