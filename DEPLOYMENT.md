# Deployment Guide

## ⚠️ Important: Docker Compose Required

This system uses **6 interconnected services**:
- API (FastAPI)
- Ingest Service (Kafka Consumer)  
- Processor (Anomaly Detection)
- Redis, Kafka, Zookeeper

**Railway/Vercel/Netlify don't support Docker Compose**. You need a platform that can run multiple containers.

## ✅ Recommended: DigitalOcean App Platform

**Best option** - Full Docker Compose support, $5/month:

1. Create a DigitalOcean account
2. Go to **App Platform**
3. Select **Docker Compose** as source
4. Connect your GitHub repo
5. Click **Deploy**

URL: https://cloud.digitalocean.com/apps

## Option 2: AWS Lightsail Containers

Deploy Docker Compose on AWS for ~$7/month:

```bash
# Install AWS CLI
# Push containers to AWS
aws lightsail push-container-image --service-name anomaly-detection --label api --image real-time-anomaly-detection-system-api
```

## Option 3: Google Cloud Run (Manual Setup)

Deploy each service separately:

1. Build and push images to Google Container Registry
2. Create 6 Cloud Run services (api, ingest, processor, redis, kafka, zookeeper)
3. Configure internal networking
4. Set environment variables

**Cost**: ~$10-15/month

## Option 4: Self-Hosted VPS (Cheapest)

Deploy on any VPS (Hetzner, Linode, Vultr) for $5/month:

```bash
# SSH into your VPS
ssh user@your-server-ip

# Install Docker
curl -fsSL https://get.docker.com | sh

# Clone your repo
git clone https://github.com/suvadityaroy/Real-Time-Anomaly-Detection-Dashboard.git
cd Real-Time-Anomaly-Detection-Dashboard

# Start services
docker-compose up -d

# Access at http://your-server-ip:8000
```

## Why Railway Failed

Railway only deployed your **API container** without Redis/Kafka. The error shows:
```
ConnectionRefusedError: [Errno 111] Connection refused (localhost:6379)
```

This means the API can't find Redis because Railway doesn't run the other containers from `docker-compose.yml`.

## Quick Fix for Railway (Not Recommended)

To make Railway work, you'd need:

1. **Create 6 separate Railway services**:
   - Service 1: API (your current deployment)
   - Service 2: Redis (Railway's built-in Redis)
   - Service 3: Kafka (use Upstash Kafka - external)
   - Service 4: Ingest (new deployment from your repo, different start command)
   - Service 5: Processor (new deployment from your repo, different start command)
   - Service 6: Zookeeper (use Upstash or external)

2. **Set environment variables** for each service to connect them via Railway's internal network

3. **Cost**: ~$5/service = **$30+/month**

## Recommended Next Steps

1. **Try DigitalOcean App Platform** (easiest, $5/month)
2. **Or use a VPS** (Hetzner Cloud - €3.79/month)
3. Delete your Railway service to avoid charges

Your system works perfectly with `docker-compose up` locally. You just need a platform that supports it.
