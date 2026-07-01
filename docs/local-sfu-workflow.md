# local-live-streamer — Workflow Diagrams

This file summarizes the current MQTT-based workflow used by local-live-streamer.

## 1. Startup

```mermaid
flowchart TD
    A([Start]) --> B[gst::init]
    B --> C[app::initialize]
    C --> D[App::run]
    D --> E[runtime::run]
    E --> F[RuntimeConfig::from_env]
    F --> G[connect_mqtt]
    G --> H[subscribe_topics]
    H --> I[AppState::new_socketio]
    I --> J[restore cameras from sfu_state.json]
    J --> K[start signaling Socket.IO thread]
    K --> L[run_mqtt_event_loop]
```

## 2. MQTT Event Processing

```mermaid
flowchart TD
    A([run_mqtt_event_loop]) --> B[msg_rx.recv]
    B -->|channel closed| Z([loop ends])
    B -->|topic + payload| C[parse CloudEvent JSON]
    C -->|parse error| D[log error]
    D --> B
    C -->|ok| E[map action + cameras]
    E --> F{action}
    F -->|create/start| G[ensure stream exists and is running]
    F -->|stop/delete| H[stop or remove stream]
    F -->|unknown| I[log warning]
    G --> B
    H --> B
    I --> B
```

## 3. Camera Pipeline Flow

```mermaid
flowchart TD
    A[Camera event] --> B[CameraStream::new or update]
    B --> C[Build GStreamer pipeline]
    C --> D[Set pipeline Playing]
    D --> E[Start ZeroMQ receiver thread]
    E --> F[Push encoded buffers to appsrc]
    F --> G[tee fanout to viewers]
    G --> H[record stream metrics]
```

## 4. Viewer Join Flow

```mermaid
sequenceDiagram
    participant Browser
    participant SigServer as Signaling Server
    participant SFU as Local SFU
    participant GST as GStreamer Tee

    Browser->>SigServer: request stream
    SigServer->>SFU: viewer_request
    SFU->>GST: create tee branch with webrtcbin
    SFU->>SigServer: webrtc_offer
    SigServer->>Browser: SDP offer
    Browser->>SigServer: SDP answer
    SigServer->>SFU: webrtc_answer
    SFU->>SFU: set remote description
```

## 5. State Persistence

```mermaid
flowchart LR
    A[Camera add/remove/update] --> B[state_manager]
    B --> C[sfu_state.json]
    C --> D[startup restore]
```

    loop ICE candidates
        Browser->>SigServer: ICE candidate
        SigServer->>SFU: webrtc_ice_candidate (viewer_id, candidate)
        SFU->>SFU: webrtcbin.add-ice-candidate
    end

    SFU-->>Browser: WebRTC video stream flowing
```

---

## 7. Viewer Disconnect

```mermaid
flowchart TD
    A[viewer_disconnect event\nfrom signaling server] --> B[find viewer_id\nin viewer_streams map]
    B -->|not found| C[log: viewer not found]
    B -->|found| D[get stream_id\nfrom viewer_streams]
    D --> E[app_state.get_camera stream_id]
    E -->|not found| F[log: camera not found]
    E -->|found| G[camera.remove_viewer\nviewer_id]
    G --> H[unlink webrtcbin from tee\ntee pad release]
    H --> I[webrtcbin.set_state Null]
    I --> J[remove from viewers map]
    J --> K[metrics: viewer_count decrement]
    K --> L[save state to sfu_state.json]
```

---

## 8. Camera Delete Flow

```mermaid
flowchart TD
    A[delete_streaming action\nfor each CameraInfo] --> B[app_state.remove_camera\nroom_name]
    B -->|not found| C[log: camera not found]
    B -->|found| D[stop all viewer branches\nfor each ViewerPeer]
    D --> E[webrtcbin.set_state Null\ntee pad release]
    E --> F[CameraStream running = false\nZMQ thread exits]
    F --> G[pipeline.set_state Null]
    G --> H[metrics::unregister_stream]
    H --> I[remove from cameras map]
    I --> J[emit socketio: stream_removed\nto signaling server]
    J --> K[save state to sfu_state.json]
```

---

## 9. Socket.IO Signaling Client Lifecycle

```mermaid
flowchart TD
    A([signaling thread starts\nos::thread::spawn]) --> B[SignalingSocketIOClient::new\nsfu_id / signaling_url / app_state]
    B --> C[loop: connect_and_run]
    C --> D[parse socket_url + namespace]
    D --> E[ClientBuilder\nWebsocket transport\nreconnect: true\nmax_attempts: 10]
    E --> F[register event handlers\non connect / on viewer_request\non webrtc_answer / on webrtc_ice_candidate\non viewer_disconnect / on sfu_register_ack]
    F --> G[client.connect]
    G -->|connect ok| H[on Connect:\nemit sfu_register]
    H --> I[on sfu_register_ack:\nregister_existing_streams\nfor all active cameras]
    I --> J[client.run_event_loop\nblocks until disconnect]
    J -->|disconnect| K[log error\nwait and retry]
    K --> C
    G -->|connect error| K
```

---

## 10. Stream Metrics (5-Second Rolling Window)

```mermaid
flowchart TD
    subgraph probes["Probe callbacks — increment atomics per room"]
        P1["ZMQ receive callback\n→ record_stream_input_frame(bytes)"]
        P2["parse.src pad probe\n→ record_stream_output_frame(bytes)"]
    end

    A([metrics task — every 5s]) --> B[list_stream_metrics]
    B --> C[for each room in DashMap]
    C --> D[elapsed_secs since last reset]
    D --> E[input_fps = input_frames / elapsed\noutput_real_fps = output_frames / elapsed]
    E --> F[input_bitrate_kbps = input_bytes * 8 / elapsed / 1000\noutput_frame_bitrate_kbps = output_frame_bytes * 8 / elapsed / 1000]
    F --> G[log StreamMetricsSnapshot]
    G --> H[reset window counters + last_reset = now]
```

---

## 11. Persistent State (sfu_state.json)

```mermaid
flowchart TD
    A([startup]) --> B[StateManager::load\nread sfu_state.json]
    B -->|file missing| C[start with empty state]
    B -->|ok| D[for each camera with status = running]
    D --> E[CameraStream::new + start]
    E --> F[register with signaling on sfu_register_ack]

    G([camera add / remove / viewer change]) --> H[StateManager::save\nwrite sfu_state.json atomically]
    H --> I[rename .json.tmp → .json\nor direct write as fallback]

    J([shutdown signal]) --> K[save final state]
    K --> L[pipeline.set_state Null for all cameras]
    L --> M([process exits])
```

---

## 12. Camera Stream States

```mermaid
stateDiagram-v2
    [*] --> Initializing: CameraStream::new

    Initializing --> Starting: add_camera called
    Starting --> Running: pipeline Playing\nZMQ thread connected
    Running --> Running: frames flowing\nviewers joining/leaving
    Running --> Stopping: delete_streaming or remove_camera
    Stopping --> [*]: pipeline Null\nmetrics unregistered

    Running --> Error: GStreamer bus error
    Error --> Stopping: cleanup triggered

    note right of Running
        status = "running"
        Saved to sfu_state.json
        Restored on restart
    end note

    note right of Error
        bus error or EOS
        pipeline auto-cleanup
    end note
```

---

## 13. Codec Auto-Detection

```mermaid
flowchart TD
    A[CameraInfo received\nfrom CloudEvent] --> B{codec field\nin event?}
    B -->|yes| C[VideoCodec::from_str\ncamera codec field]
    B -->|no| D{camera_name\ncontains h264 / h265?}
    D -->|yes| E[VideoCodec::detect_from_camera_name]
    D -->|no| F{DEFAULT_VIDEO_CODEC\nenv var set?}
    F -->|yes| G[VideoCodec::from_str\nenv value]
    F -->|no| H[default: VideoCodec::H264]
    C --> I[start pipeline with detected codec]
    E --> I
    G --> I
    H --> I
    I --> J[ZMQ first frame arrives]
    J --> K[VideoCodec::from_video_data\nNAL header inspection]
    K -->|differs from pipeline codec| L[log: codec mismatch\nupdate & rebuild caps]
    K -->|matches| M[codec_detected_from_stream = true]
    L --> M
```
