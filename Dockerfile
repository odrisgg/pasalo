FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json ./
# Instalación determinista según el lockfile, sin dependencias de desarrollo.
RUN npm ci --omit=dev
COPY --chown=node:node server.js ./
COPY --chown=node:node public ./public
# /app escribible para tokens.json y /app/data para montarlo como volumen.
RUN mkdir -p /app/data && chown -R node:node /app
# No correr como root dentro del contenedor.
USER node
EXPOSE 3000
CMD ["node", "server.js"]
