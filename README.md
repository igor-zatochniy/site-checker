# 🚀 Concurrent Site Checker

High-performance Go utility for real-time website availability monitoring.
The project demonstrates the power of **Go Concurrency (Goroutines & Channels)** to process multiple network requests in parallel and is packaged in **Docker** for cross-platform deployment.

## 📋 Features

- **Concurrency:** checks hundreds of websites simultaneously using Goroutines and Channels.
- **Dockerized:** Fully isolated environment. Runs on any OS (Linux, Windows, macOS) without installing Go.
- **Optimized:** Uses Docker **Multi-stage build**. Final image size is under 20MB.
- **HTTPS Support:** Configured with root SSL certificates for Alpine Linux.

## 🛠 Tech Stack

- **Language:** Go (Golang) 1.25.4
- **Concurrency Pattern:** Worker Pool / Unbuffered Channels
- **Containerization:** Docker
- **Base Image:** Alpine Linux

## 🚀 How to Run

### Option 1: Docker (Recommended)

No local Go installation required.

1.  **Build the image:**

    ```bash
    docker build -t site-checker .
    ```

2.  **Run the container:**
    ```bash
    docker run --rm --name checker site-checker
    ```

### Option 2: Local Run

Requires Go installed on your machine.

1.  **Install dependencies:**

    ```bash
    go mod tidy
    ```

2.  **Run the application:**
    ```bash
    go run main.go
    ```

## 📂 Project Structure

```text
.
├── main.go       # Core application logic (Goroutines, HTTP requests)
├── Dockerfile    # Multi-stage build instructions for Docker
├── go.mod        # Go module definition and dependencies
├── .gitignore    # Git configuration to ignore build artifacts/secrets
└── README.md     # Project documentation
```
