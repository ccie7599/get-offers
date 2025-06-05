# Use an official Node.js runtime as the base image
FROM node:20-slim

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy package.json and package-lock.json first for caching layer
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the application code
COPY . .

# Create the data directory inside the container
RUN mkdir -p /tmp/data

# Expose the HTTP port
EXPOSE 3001

# Define the default command
CMD ["node", "get-offers.js"]
