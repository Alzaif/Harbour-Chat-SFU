FROM node:20-bookworm-slim

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install

COPY src ./src

ENV PORT=4000
EXPOSE 4000

CMD ["npm", "start"]
