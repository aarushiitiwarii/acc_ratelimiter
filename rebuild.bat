kind create cluster --name edge
docker build -f Dockerfile.baseline -t baseline:v1 .
docker build -f Dockerfile.gateway -t gateway:v2 .
kind load docker-image baseline:v1 --name edge
kind load docker-image gateway:v2 --name edge
kubectl apply -f redis.yaml
kubectl apply -f baseline.yaml
kubectl apply -f gateway.yaml
kubectl set image deploy/gateway gateway=gateway:v2
kubectl set env deploy/gateway REPLICAS=3 BATCH_SIZE=50 LIMIT_RATE=5000 LIMIT_CAPACITY=5000