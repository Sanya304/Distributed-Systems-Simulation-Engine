# ---------------------------------------------------------------------------
# Distributed Simulation Engine — React frontend
#
# Two-stage build:
#   Stage 1 (builder): install node_modules, run `vite build`
#   Stage 2 (nginx):   copy only the compiled dist/ — no node_modules in prod image
#
# The nginx config rewrites all paths to index.html so React Router works,
# and proxies /api and /ws to the backend container.
# ---------------------------------------------------------------------------

# ── Stage 1: build ──────────────────────────────────────────────────────────
FROM node:22-alpine AS builder

WORKDIR /app

# Install deps first for better layer caching.
COPY package.json package-lock.json ./
RUN npm ci --silent

# Copy source and build.
COPY index.html        .
COPY vite.config.ts    .
COPY tsconfig*.json    ./
COPY src/              src/
COPY public/           public/

RUN npm run build


# ── Stage 2: serve ──────────────────────────────────────────────────────────
FROM nginx:alpine

# Remove the default nginx site config.
RUN rm /etc/nginx/conf.d/default.conf

# Our custom nginx config (written inline below via a HEREDOC alternative —
# we use a COPY so it's easy to read and version-control).
COPY nginx.conf /etc/nginx/conf.d/app.conf

# Copy built assets from the builder stage.
COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
