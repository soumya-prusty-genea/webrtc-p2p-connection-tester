pub use crate::events::VideoCodec;

pub fn create_video_caps(codec: &VideoCodec) -> &'static str {
    codec.get_gstreamer_caps()
}
