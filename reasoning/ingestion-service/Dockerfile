# Use base image for installing dependencies and transpiling Typescript to Javascript
FROM node:12.14 as builder
WORKDIR /usr/src/app
COPY ./package.json ./
RUN npm install
COPY . .
RUN npm run build

# Use alpine lightweight version for running the application. Copies generated sources from the first
# intermediate container into this one.
FROM node:12.14-alpine
WORKDIR /usr/src/app
COPY --from=builder /usr/src/app ./
EXPOSE 3000
CMD ["npm", "run", "start:prod"]