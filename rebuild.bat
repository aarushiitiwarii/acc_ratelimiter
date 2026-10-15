@echo off
rem Build all images, load them into the kind cluster and (re)deploy.
rem Safe to re-run: the cluster is only created if missing, limits come from
rem the manifests (no ad-hoc "set env"), and images use the fixed tag :demo
rem (imagePullPolicy: Never) so a rollout restart picks up the new build.
setlocal
set CLUSTER=edge
if "%AUTH_SECRET%"=="" set AUTH_SECRET=demo123
if "%ADMIN_TOKEN%"=="" set ADMIN_TOKEN=admin123

kind get clusters | findstr /b /c:"%CLUSTER%" >nul || kind create cluster --name %CLUSTER% --config kind-config.yaml || exit /b 1
kubectl config use-context kind-%CLUSTER% || exit /b 1

docker build -q -f Dockerfile.gateway  -t gateway:demo  . || exit /b 1
docker build -q -f Dockerfile.baseline -t baseline:demo . || exit /b 1
docker build -q -f Dockerfile          -t hello:demo    . || exit /b 1
docker build -q -f Dockerfile.console  -t console:demo  . || exit /b 1
kind load docker-image gateway:demo baseline:demo hello:demo console:demo --name %CLUSTER% || exit /b 1

kubectl create secret generic rateguard --from-literal=auth-secret=%AUTH_SECRET% ^
  --from-literal=admin-token=%ADMIN_TOKEN% --dry-run=client -o yaml | kubectl apply -f - || exit /b 1
kubectl apply -f redis.yaml -f hello.yaml -f baseline.yaml -f gateway.yaml -f console.yaml || exit /b 1
kubectl rollout restart deployment/hello deployment/baseline deployment/gateway deployment/console || exit /b 1
for %%d in (redis hello baseline gateway console) do kubectl rollout status deployment/%%d --timeout=120s || exit /b 1
echo.
echo Deployed. Run demo.bat to open the console.
