# --- build stage: reproducible install from the lockfile, then a production build ---
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY index.html vite.config.js ./
COPY src ./src
RUN npm run build

# --- runtime stage: unprivileged nginx serving static files and proxying /api ---
FROM nginxinc/nginx-unprivileged:1.27-alpine
# *.template files are run through envsubst at container start and written to
# /etc/nginx/conf.d/ -- only the variables below are substituted, so nginx's own
# $variables in the file are left alone.
COPY nginx/default.conf.template /etc/nginx/templates/default.conf.template
ENV NGINX_ENVSUBST_FILTER="^(PRODUCER_UPSTREAM|CONSUMER_UPSTREAM|DNS_RESOLVER)$" \
    PRODUCER_UPSTREAM="http://producer:8080" \
    CONSUMER_UPSTREAM="http://consumer:8080" \
    DNS_RESOLVER="127.0.0.11"
COPY nginx/security-headers.conf /etc/nginx/snippets/security-headers.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
