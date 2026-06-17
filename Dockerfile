FROM node:20-bookworm-slim

# Install g++ and OpenJDK (Java)
RUN apt-get update && \
    apt-get install -y --no-install-recommends g++ default-jdk && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

# Expose the health-check port
EXPOSE 3000

CMD ["npm", "run", "worker"]
