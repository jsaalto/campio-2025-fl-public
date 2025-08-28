
# az login

$containerregistry = "campio2025flcr"

az acr login --name $containerregistry

$containerimage = "campio2025flcr.azurecr.io/ingestion-agent-aca:v0"

docker build --pull --rm -f "Dockerfile" -t ingestion-agent-aca:latest .
docker tag ingestion-agent-aca:latest $containerimage
docker push $containerimage

$resourcegroup = "CampIO-2025-FL-westus"
$containerapp = "mcp-server-backend-tools"

az containerapp update `
    --name $containerapp `
    --image $containerimage `
    --resource-group $resourcegroup

# $containerappenv = "campio2025flcntappenv"
# $acrusername = az acr credential show --name $containerregistry --query "username" --output tsv
# $acrpassword = az acr credential show --name $containerregistry --query "passwords[0].value" --output tsv
# 
# az containerapp create `
#     --name $containerapp `
#     --resource-group $resourcegroup `
#     --environment $containerappenv `
#     --image $containerimage `
#     --cpu 0.5 `
#     --memory 1.0Gi `
#     --registry-server "$containerregistry.azurecr.io" `
#     --registry-username $acrusername `
#     --registry-password $acrpassword `
#     --ingress 'external' `
#     --target-port 80
