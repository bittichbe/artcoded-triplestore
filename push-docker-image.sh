set -e
docker build -t artcoded/triplestore .
docker tag artcoded/triplestore artcoded:5000/artcoded/triplestore
docker push artcoded:5000/artcoded/triplestore
