//! WebRTC element construction for a per-viewer send branch.
//!
//! This was previously inlined in the `camera_manager` monolith. It builds the
//! `queue -> parse -> payload -> capsfilter -> webrtcbin` chain for a single
//! viewer and configures STUN/TURN plus RTCP loss-resilience feedback.

use anyhow::Result;
use gstreamer as gst;
use gstreamer::prelude::*;
use log::info;

use crate::events::VideoCodec;

/// Read a boolean env var with a default (mirrors `camera_manager::env_bool`).
fn env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

/// The five elements that make up a viewer's WebRTC send branch.
pub struct WebRtcElements {
    pub queue: gst::Element,
    pub parser: gst::Element,
    pub payloader: gst::Element,
    pub capsfilter: gst::Element,
    pub webrtcbin: gst::Element,
}

/// Build and configure the per-viewer WebRTC send chain.
pub fn build_webrtc_elements(
    codec: &VideoCodec,
    stun_server: &str,
    turn_servers: &[String],
    viewer_id: &str,
) -> Result<WebRtcElements> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let is_h264 = *codec == VideoCodec::H264;
    let pay_element = if is_h264 { "rtph264pay" } else { "rtph265pay" };
    let encoding_name = if is_h264 { "H264" } else { "H265" };

    let queue_name = format!("viewer-queue-{}-{}", viewer_id, timestamp);
    let parse_name = format!("viewer-{}-{}-{}", codec.get_parser_element(), viewer_id, timestamp);
    let pay_name = format!("viewer-{}-{}-{}", pay_element, viewer_id, timestamp);
    let capsfilter_name = format!("viewer-capsfilter-{}-{}", viewer_id, timestamp);
    let webrtc_name = format!("viewer-webrtcbin-{}-{}", viewer_id, timestamp);

    let queue = gst::ElementFactory::make("queue")
        .property("max-size-buffers", 10u32)
        .property_from_str("leaky", "downstream")
        .name(&queue_name)
        .build()?;

    let parser = gst::ElementFactory::make(codec.get_parser_element())
        .property("config-interval", 1i32)
        .name(&parse_name)
        .build()?;

    let payloader = gst::ElementFactory::make(pay_element)
        .property("pt", 102u32)
        .property("config-interval", 1i32)
        .property_from_str("aggregate-mode", "zero-latency")
        .name(&pay_name)
        .build()?;

    let capsfilter = gst::ElementFactory::make("capsfilter")
        .name(&capsfilter_name)
        .build()?;

    // Advertise RTCP feedback in the RTP caps so webrtcbin negotiates
    // loss-resilience (NACK/RTX, PLI/FIR keyframe requests, transport-cc) in
    // the SDP. Toggle off with WEBRTC_LOSS_RESILIENCE=0.
    let mut caps_builder = gst::Caps::builder("application/x-rtp")
        .field("media", "video")
        .field("encoding-name", encoding_name)
        .field("payload", 102i32);
    if env_bool("WEBRTC_LOSS_RESILIENCE", true) {
        caps_builder = caps_builder
            .field("rtcp-fb-nack", true)
            .field("rtcp-fb-nack-pli", true)
            .field("rtcp-fb-ccm-fir", true)
            .field("rtcp-fb-transport-cc", true);
    }
    capsfilter.set_property("caps", &caps_builder.build());

    let webrtcbin = gst::ElementFactory::make("webrtcbin")
        .property_from_str("bundle-policy", "max-bundle")
        .name(&webrtc_name)
        .build()?;

    webrtcbin.set_property("stun-server", stun_server);
    for turn_server in turn_servers {
        let _ = webrtcbin.emit_by_name::<bool>("add-turn-server", &[&turn_server.as_str()]);
        info!(" Added TURN server: {}", turn_server);
    }

    Ok(WebRtcElements {
        queue,
        parser,
        payloader,
        capsfilter,
        webrtcbin,
    })
}
