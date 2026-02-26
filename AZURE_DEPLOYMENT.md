# Deploying to Azure - Complete Guide

This guide walks you through deploying your Stockholm Traffic Dashboard (Streamlit) and Dagster orchestrator to Azure.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│              Azure Resource Group                        │
│  (stockholm-traffic-dashbord-rg)                        │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌──────────────────┐          ┌──────────────────┐     │
│  │  App Service     │          │  Container       │     │
│  │  (Dashboard)     │          │  Instances       │     │
│  │  Streamlit       │          │  (Dagster)       │     │
│  │  Port 8501       │          │  Port 3000       │     │
│  └──────────────────┘          └──────────────────┘     │
│           ↑                              ↑                │
│           └──────────────┬───────────────┘                │
│                          │                                │
│         ┌────────────────▼────────────────┐              │
│         │  Azure Container Registry (ACR) │              │
│         │  stockholm-dagster:latest       │              │
│         │  stockholm-dashboard:latest     │              │
│         └─────────────────────────────────┘              │
│                          ▲                                │
│         ┌────────────────┴────────────────┐              │
│         │  Azure Files / Storage Account  │              │
│         │  warehouse/                     │              │
│         └─────────────────────────────────┘              │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

## Prerequisites

1. **Azure Account** with active subscription
2. **Azure CLI** installed ([download](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli))
3. **Docker** installed locally (for testing) - optional but recommended
4. **Git** installed
5. **Resource Group** created in Azure (you already have `stockholm-traffic-dashbord-rg`)

## Step-by-Step Deployment

### Step 1: Prepare Environment Variables

```bash
# Copy the example env file
cp .env.example .env

# Edit .env with your Azure details
# Use your text editor to fill in:
# - AZURE_SUBSCRIPTION_ID
# - AZURE_CONTAINER_REGISTRY_NAME (must be globally unique, lowercase)
# - AZURE_REGION (e.g., northeurope, eastus)
```

**Important:** Container registry names must:
- Be 5-50 characters
- Contain only lowercase letters, numbers
- Be globally unique across Azure

### Step 2: Login to Azure

```bash
# Login to Azure (opens browser for authentication)
az login

# Verify you're using the correct subscription
az account set --subscription "<your-subscription-id>"

# Verify resource group exists
az group show --name stockholm-traffic-dashbord-rg
```

### Step 3: Build and Push Docker Images to Azure Container Registry

```bash
# Make script executable (Linux/Mac)
chmod +x deploy-to-azure.sh

# Run the deployment script
./deploy-to-azure.sh
```

**What this does:**
- ✅ Creates Azure Container Registry
- ✅ Authenticates with the registry
- ✅ Builds Dagster Docker image in the cloud (faster than local build)
- ✅ Builds Dashboard Docker image in the cloud
- ✅ Stores images for deployment

**Time estimate:** 5-10 minutes

### Step 4: Deploy Dashboard to App Service

```bash
chmod +x deploy-app-service.sh

./deploy-app-service.sh
```

**What this does:**
- ✅ Creates Azure Storage Account (for shared warehouse data)
- ✅ Creates Azure File Share
- ✅ Creates App Service Plan (Linux, B1 tier)
- ✅ Creates Web App with Streamlit container
- ✅ Configures auto-scaling and health checks
- ✅ Mounts storage volume

**Configuration:**
- **App Plan:** B1 (1 core, 1.75 GB RAM) - suitable for Streamlit
- **Storage:** Standard LRS (geo-redundant if production)
- **Auto-scaling:** Can be configured based on CPU/Memory

**Output:** Dashboard will be available at `https://stockholm-traffic-dashboard.azurewebsites.net`

### Step 5: Deploy Dagster to Container Instances

```bash
chmod +x deploy-container-instances.sh

./deploy-container-instances.sh
```

**What this does:**
- ✅ Creates Container Instance for Dagster scheduler
- ✅ Configures registry credentials
- ✅ Mounts shared storage (warehouse)
- ✅ Sets environment variables
- ✅ Configures restart policy

**Configuration:**
- **CPU:** 1 vCPU
- **Memory:** 2 GB
- **Restart policy:** OnFailure (retries on crash)
- **Volume:** /app/warehouse mounted from Azure Files

**To get Dagster IP address:**
```bash
az container show \
  -n stockholm-dagster \
  -g stockholm-traffic-dashbord-rg \
  --query ipAddress.ip
```

Dagster will be at: `http://<returned-ip>:3000`

## Verification & Testing

### Check Dashboard Status
```bash
# Get dashboard URL
az webapp browse --name stockholm-traffic-dashboard --resource-group stockholm-traffic-dashbord-rg

# View real-time logs
az webapp log tail -n stockholm-traffic-dashboard -g stockholm-traffic-dashbord-rg
```

### Check Dagster Status
```bash
# Get container status
az container show -n stockholm-dagster -g stockholm-traffic-dashbord-rg --query instanceView.state

# View logs
az container logs -n stockholm-dagster -g stockholm-traffic-dashbord-rg

# Get IP address
az container show -n stockholm-dagster -g stockholm-traffic-dashbord-rg --query ipAddress.ip
```

### Test Deployed Dashboard
```bash
# From local machine
curl https://stockholm-traffic-dashboard.azurewebsites.net
```

## Updating Deployments

### Update Dashboard
```bash
# Rebuild and push new image
az acr build \
  --registry <your-registry> \
  --image stockholm-dashboard:latest \
  --file Dockerfile.dashboard .

# Restart web app (pulls latest image)
az webapp restart -n stockholm-traffic-dashboard -g stockholm-traffic-dashbord-rg
```

### Update Dagster
```bash
# Rebuild and push new image
az acr build \
  --registry <your-registry> \
  --image stockholm-dagster:latest \
  --file Dockerfile.dagster .

# Delete old container
az container delete -n stockholm-dagster -g stockholm-traffic-dashbord-rg --yes

# Redeploy
./deploy-container-instances.sh
```

## Troubleshooting

### Dashboard shows "502 Bad Gateway"
```bash
# Check if app is actually running
az webapp show -n stockholm-traffic-dashboard -g stockholm-traffic-dashbord-rg --query state

# View logs for errors
az webapp log tail -n stockholm-traffic-dashboard -g stockholm-traffic-dashbord-rg
```

### Dagster container won't start
```bash
# Check logs
az container logs -n stockholm-dagster -g stockholm-traffic-dashbord-rg

# Verify image exists in registry
az acr repository show --name <registry-name> --repository stockholm-dagster

# Check if storage is mounted correctly
az storage share list --account-name stockholmtrafficsa
```

### Storage mount issues
```bash
# Verify storage account exists
az storage account show --name stockholmtrafficsa --resource-group stockholm-traffic-dashbord-rg

# Check file share
az storage share list --account-name stockholmtrafficsa --account-key <storage-key>

# Upload test file
az storage file upload \
  --account-name stockholmtrafficsa \
  --share-name warehouse \
  --source test.txt
```

## Cost Estimation (Monthly, Azure Pricing)

| Resource | Tier | Cost/Month |
|----------|------|-----------|
| App Service | B1 (Basic) | ~$10 |
| Container Instances | 1 vCPU, 2GB RAM | ~$15-30 |
| Storage Account | 1GB data | ~$1-5 |
| Container Registry | Basic | ~$5 |
| **Total** | | **~$30-50** |

*Actual costs depend on data transfer and exact usage*

## Security Best Practices

1. **Enable HTTPS** on App Service (automatic via `.azurewebsites.net`)
2. **Use Managed Identity** instead of connection strings:
   ```bash
   az webapp identity assign -n stockholm-traffic-dashboard -g stockholm-traffic-dashbord-rg
   ```

3. **Restrict Container Registry access:**
   ```bash
   az acr update --name <registry> --default-action Deny
   az acr network-rule add --name <registry> --resource-group <rg> --virtual-network <vnet-id> --subnet <subnet-id>
   ```

4. **Use Key Vault** for sensitive environment variables:
   ```bash
   az keyvault create --name stockholm-traffic-kv --resource-group stockholm-traffic-dashbord-rg
   ```

5. **Enable monitoring:** Set up Azure Monitor alerts for failing health checks

## Next Steps

- [ ] Monitor resource usage and adjust App Service tier if needed
- [ ] Set up CI/CD pipeline (GitHub Actions → Azure Container Registry → Deploy)
- [ ] Enable auto-scaling on App Service based on CPU/memory
- [ ] Configure backup for database in Azure Files
- [ ] Set up monitoring and alerting
- [ ] Check Azure Advisor recommendations for cost optimization

## Additional Resources

- [Azure App Service Documentation](https://learn.microsoft.com/en-us/azure/app-service/)
- [Azure Container Instances](https://learn.microsoft.com/en-us/azure/container-instances/)
- [Azure Container Registry](https://learn.microsoft.com/en-us/azure/container-registry/)
- [Azure Storage Files](https://learn.microsoft.com/en-us/azure/storage/files/)
- [Azure Pricing Calculator](https://azure.microsoft.com/en-us/pricing/calculator/)
