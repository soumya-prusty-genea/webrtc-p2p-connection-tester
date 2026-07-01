/// A key-value label for tagging a metric data point.
#[derive(Clone)]
pub struct MetricLabel {
    pub name: String,
    pub value: String,
}

/// A single metric data point collected locally.
pub struct MetricPoint {
    pub metric_name: String,
    pub value: f64,
    pub unit: Option<String>,
    pub labels: Option<Vec<MetricLabel>>,
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

/// Trait for collecting metrics locally (no external publishing).
pub trait MetricsProvider: Send + Sync {
    fn collect_metrics(&self) -> Vec<MetricPoint>;
}

pub struct LocalSfuMetricsProvider;

impl LocalSfuMetricsProvider {
    pub fn new() -> Self {
        Self
    }

    fn metric(
        metric_name: String,
        value: f64,
        unit: Option<&str>,
        labels: Option<Vec<MetricLabel>>,
    ) -> MetricPoint {
        MetricPoint {
            metric_name,
            value,
            unit: unit.map(str::to_string),
            labels,
            timestamp: None,
        }
    }
}

impl MetricsProvider for LocalSfuMetricsProvider {
    fn collect_metrics(&self) -> Vec<MetricPoint> {
        let snapshots = super::list_stream_metrics();
        let mut points = Vec::with_capacity(snapshots.len() * 12);

        for snapshot in snapshots {
            let stream_labels = vec![
                MetricLabel {
                    name: "StreamId".to_string(),
                    value: snapshot.stream_id.clone(),
                },
                MetricLabel {
                    name: "RoomName".to_string(),
                    value: snapshot.room_name.clone(),
                },
                MetricLabel {
                    name: "CameraUUID".to_string(),
                    value: snapshot.camera_uuid.clone(),
                },
            ];

            points.push(Self::metric(
                "sfu.stream.input.fps".to_string(),
                snapshot.input_fps,
                Some("None"),
                Some(stream_labels.clone()),
            ));
            points.push(Self::metric(
                "sfu.stream.output.fps".to_string(),
                snapshot.output_real_fps,
                Some("None"),
                Some(stream_labels.clone()),
            ));
            points.push(Self::metric(
                "sfu.stream.output.packet_rate".to_string(),
                snapshot.output_packet_rate,
                Some("Count/Second"),
                Some(stream_labels.clone()),
            ));
            points.push(Self::metric(
                "sfu.stream.input.bitrate.kbps".to_string(),
                snapshot.input_bitrate_kbps,
                Some("Kilobits/Second"),
                Some(stream_labels.clone()),
            ));
            points.push(Self::metric(
                "sfu.stream.output.frame_bitrate.kbps".to_string(),
                snapshot.output_frame_bitrate_kbps,
                Some("Kilobits/Second"),
                Some(stream_labels.clone()),
            ));
            points.push(Self::metric(
                "sfu.stream.output.packet_bitrate.kbps".to_string(),
                snapshot.output_packet_bitrate_kbps,
                Some("Kilobits/Second"),
                Some(stream_labels.clone()),
            ));
            points.push(Self::metric(
                "sfu.stream.input.frames.total".to_string(),
                snapshot.input_frames_total as f64,
                Some("Count"),
                Some(stream_labels.clone()),
            ));
            points.push(Self::metric(
                "sfu.stream.output.frames.total".to_string(),
                snapshot.output_frames_total as f64,
                Some("Count"),
                Some(stream_labels.clone()),
            ));
            points.push(Self::metric(
                "sfu.stream.output.packets.total".to_string(),
                snapshot.output_packets_total as f64,
                Some("Count"),
                Some(stream_labels.clone()),
            ));
            points.push(Self::metric(
                "sfu.stream.input.bytes.total".to_string(),
                snapshot.input_bytes_total as f64,
                Some("Bytes"),
                Some(stream_labels.clone()),
            ));
            points.push(Self::metric(
                "sfu.stream.output.frame_bytes.total".to_string(),
                snapshot.output_frames_bytes_total as f64,
                Some("Bytes"),
                Some(stream_labels.clone()),
            ));
            points.push(Self::metric(
                "sfu.stream.output.packet_bytes.total".to_string(),
                snapshot.output_packets_bytes_total as f64,
                Some("Bytes"),
                Some(stream_labels),
            ));
        }

        points
    }
}
