# Self-contained build: Debian apt GStreamer (no private ECR base image required).
FROM rust:1-trixie AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libclang-dev \
    clang \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    libgstreamer-plugins-bad1.0-dev \
    libglib2.0-dev \
    libssl-dev \
    libsoup-3.0-dev \
    libsrtp2-dev \
    libzmq3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release && \
    strip target/release/webrtc-connection-tester

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libzmq5 \
    libssl3 \
    libglib2.0-0 \
    libsoup-3.0-0 \
    libsrtp2-1 \
    gstreamer1.0-tools \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    gstreamer1.0-nice \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

RUN gst-launch-1.0 --version && \
    gst-inspect-1.0 nice && \
    gst-inspect-1.0 webrtcbin

COPY --from=builder /app/target/release/webrtc-connection-tester /usr/local/bin/webrtc-connection-tester
COPY config/ /app/config/

# Small looping H264 clip for USE_TEST_STREAM mode (no live encoder plugin required).
RUN mkdir -p /app/assets && \
    ffmpeg -y -f lavfi -i testsrc=size=640x360:rate=30 -t 2 \
    -c:v libx264 -pix_fmt yuv420p -g 30 -keyint_min 30 \
    -f h264 /app/assets/test_clip.h264

WORKDIR /app

CMD ["webrtc-connection-tester"]
