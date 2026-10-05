repo=YOUR_REPO_NAME
version=YOUR_VERSION

DOCKER_BUILDKIT=0 docker buildx build \
  --platform linux/amd64 \
  --load \
  -f Dockerfile_2 \
  -t ${repo}:${version}-amd . 2>&1 | tee ../out_amd_${version}.log
echo ${repo}:${version}-amd

docker push ${repo}:${version}-amd
echo ${repo}:${version}-amd