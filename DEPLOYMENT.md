# Deployment Guide

## Quick Deploy with Railway.app

1. **Install Railway CLI**:
   ```powershell
   npm install -g @railway/cli
   ```

2. **Login**:
   ```powershell
   railway login
   ```

3. **Initialize Project**:
   ```powershell
   railway init
   ```

4. **Add Services** (in Railway dashboard):
   - Redis (built-in)
   - Kafka (or use Upstash Kafka)
   
5. **Set Environment Variables**:
   ```
   REDIS_URL=<from Railway Redis>
   KAFKA_BROKER=<from Kafka service>
   KAFKA_TOPIC=events
   ```

6. **Deploy**:
   ```powershell
   railway up
   ```

Your app will be live at: `https://your-app.up.railway.app`

## Alternative: Fly.io

```powershell
# Install
powershell -Command "iwr https://fly.io/install.ps1 -useb | iex"

# Deploy
fly launch
fly deploy
```

## Alternative: Render.com

1. Push code to GitHub
2. Go to https://render.com
3. New → Web Service
4. Connect repo
5. Add Redis service
6. Deploy

## Notes

- Vercel doesn't support Docker/long-running services
- Use Railway, Fly.io, or Render for this type of app
- All support Docker and have free tiers
