# 🚀 Concurrent Site Checker

High-performance Go utility for real-time website availability monitoring.
This project demonstrates the efficient use of **Go Concurrency (Goroutines & Channels)** to process multiple network requests in parallel and is fully containerized using **Docker** for seamless cross-platform deployment.

## 📋 Features

- **High Performance:** Monitors over 100 websites simultaneously using a custom concurrent architecture.
- **Worker Pool Pattern:** Efficiently limits system resources by reusing a fixed number of workers.
- **Ticker-based Monitoring:** Runs active checking waves periodically every 5 seconds.
- **Dockerized:** Fully isolated environment. Runs on any OS (Linux, Windows, macOS) without requiring a local Go installation.
- **Production-Optimized Build:** Uses Docker **Multi-stage builds** to keep the final production image size under 20MB.
- **Secure HTTPS Support:** Configured with root SSL certificates inside the Alpine Linux environment for secure requests.

## 🛠 Tech Stack

- **Language:** Go (Golang) 1.24+
- **Concurrency Pattern:** Worker Pool / Buffered Channels (Producer-Consumer architecture)
- **Containerization:** Docker
- **Base Image:** Alpine Linux

## 🚀 How to Run

### Option 1: Docker (Recommended)

No local Go installation required.

1. **Build the Docker image:**
   ```bash
   docker build -t site-checker .

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
├── main.go       # Core application logic (Goroutines, Worker Pool, HTTP Client)
├── Dockerfile    # Production-optimized multi-stage build instructions
├── go.mod        # Go module definition and dependencies
├── .gitignore    # Git configuration to ignore build artifacts and local secrets
└── README.md     # Project documentation and instructions
```
