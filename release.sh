set -e # fail script on error

if [[ -z "$1" || -z "$2" ]]
then
  echo "version mut be provided"
  exit -1;
fi

releaseVersion=$1
nextVersion=$2-SNAPSHOT

echo "release" $releaseVersion ", next" $nextVersion

mvn --batch-mode -Dtag=$releaseVersion release:prepare \
                 -DreleaseVersion=$releaseVersion \
                 -DdevelopmentVersion=$nextVersion

mvn release:clean
git pull

git checkout $releaseVersion
docker build -t artcoded/triplestore:v$releaseVersion .
docker tag artcoded/triplestore:v$releaseVersion artcoded:5000/artcoded/triplestore:v$releaseVersion
docker push artcoded:5000/artcoded/triplestore:v$releaseVersion

git checkout main
