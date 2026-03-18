# Dockerfile.app
# Multi-stage build: development (hot reload) + production (optimised)

# ── Base ───────────────────────────────────────────────────────
FROM node:20-alpine AS base
WORKDIR /app
RUN apk add --no-cache libc6-compat curl
COPY package*.json ./

# ── Development (mounted volume, hot reload) ──────────────────
FROM base AS development
ENV NODE_ENV=development
# Copy package files first so this layer is cached separately from source
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 3000
CMD ["npm", "run", "dev"]

# ── Builder (for production) ──────────────────────────────────
FROM base AS builder
ENV NODE_ENV=production
RUN npm ci --only=production
COPY . .
RUN npm run build

# ── Production (minimal image) ────────────────────────────────
FROM node:20-alpine AS production
WORKDIR /app
ENV NODE_ENV=production

RUN addgroup --system --gid 1001 nodejs && \
    adduser  --system --uid 1001 nextjs

COPY --from=builder /app/public        ./public
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static  ./.next/static

USER nextjs
EXPOSE 3000
ENV PORT=3000
CMD ["node", "server.js"]
