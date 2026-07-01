use anyhow::Result;
use gstreamer as gst;

fn main() -> Result<()> {
    gst::init()?;
    let app = webrtc_connection_tester::app::initialize()?;
    app.run()
}
