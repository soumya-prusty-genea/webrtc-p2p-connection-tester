// LiveKit room player — generates its own access token client-side from
// API key/secret so this stays a standalone test page with no backend.

let room = null;

// DOM elements
const joinButton = document.getElementById('join-btn');
const leaveButton = document.getElementById('leave-btn');
const videoContainer = document.getElementById('video-container');
const statusEl = document.getElementById('status');
const roomInput = document.getElementById('lk-room');
const cameraUuidInput = document.getElementById('camera-uuid');

function setStatus(msg) {
  statusEl.textContent = msg;
  console.log(msg);
}

function getRoomName() {
  const cameraUuid = cameraUuidInput.value.trim();
  return 'room-' + cameraUuid;
}

// Keep the derived room name field in sync as the user types the camera UUID
cameraUuidInput.addEventListener('input', () => {
  roomInput.value = getRoomName();
});
roomInput.value = getRoomName();

function base64url(bytes) {
  let base64;
  if (typeof bytes === 'string') {
    base64 = btoa(unescape(encodeURIComponent(bytes)));
  } else {
    base64 = btoa(String.fromCharCode(...new Uint8Array(bytes)));
  }
  return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

async function hmacSha256(secret, data) {
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  return crypto.subtle.sign('HMAC', key, new TextEncoder().encode(data));
}

// Builds a LiveKit access token (HS256 JWT) directly in the browser.
async function buildLiveKitToken({ apiKey, apiSecret, room, identity, ttlSeconds = 3600 }) {
  const header = { typ: 'JWT', alg: 'HS256' };
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    video: {
      roomJoin: true,
      room: room,
      canPublish: true,
      canSubscribe: true,
      canPublishData: true,
    },
    sub: identity,
    iss: apiKey,
    nbf: now,
    exp: now + ttlSeconds,
  };

  const encodedHeader = base64url(JSON.stringify(header));
  const encodedPayload = base64url(JSON.stringify(payload));
  const signingInput = encodedHeader + '.' + encodedPayload;
  const signature = await hmacSha256(apiSecret, signingInput);
  return signingInput + '.' + base64url(signature);
}

// Event listeners for buttons
joinButton.addEventListener('click', joinRoom);
leaveButton.addEventListener('click', leaveRoom);

// Join room function
async function joinRoom() {
  try {
    const wsUrl = document.getElementById('lk-url').value.trim();
    const apiKey = document.getElementById('lk-api-key').value.trim();
    const apiSecret = document.getElementById('lk-api-secret').value.trim();
    const identity = document.getElementById('lk-identity').value.trim() || ('viewer-' + Date.now());
    const roomName = getRoomName();
    roomInput.value = roomName;

    if (!wsUrl || !apiKey || !apiSecret || !cameraUuidInput.value.trim()) {
      setStatus('Fill in LiveKit URL, API key, API secret, and Camera UUID first.');
      return;
    }

    setStatus('Generating token for room "' + roomName + '"...');
    const token = await buildLiveKitToken({ apiKey, apiSecret, room: roomName, identity });

    room = new LivekitClient.Room({
      adaptiveStream: true,
      dynacast: true,
    });
    setupRoomListeners();

    setStatus('Connecting to ' + wsUrl + ' ...');
    await room.connect(wsUrl, token);
    setStatus('Connected to room: ' + room.name + ' as ' + identity);

    // Update button states
    joinButton.disabled = true;
    leaveButton.disabled = false;

    // Render all existing remote participants
    room.remoteParticipants.forEach((participant) => {
      console.log('Rendering existing remote participant:', participant.identity);
      renderParticipant(participant);
    });

    // Render local participant (viewer-only: not publishing camera/mic by default)
    renderParticipant(room.localParticipant);
  } catch (error) {
    console.error('Error joining room:', error);
    setStatus('Error joining room: ' + error.message);
  }
}

// Leave room function
async function leaveRoom() {
  try {
    if (room) {
      await room.disconnect();
      room = null;
    }
    setStatus('Disconnected from room');

    // Update button states
    joinButton.disabled = false;
    leaveButton.disabled = true;

    // Clear video container
    videoContainer.innerHTML = '';
  } catch (error) {
    console.error('Error leaving room:', error);
    setStatus('Error leaving room: ' + error.message);
  }
}

// Set up room event listeners
function setupRoomListeners() {
  // When a new participant connects
  room.on(LivekitClient.RoomEvent.ParticipantConnected, (participant) => {
    console.log('Participant connected:', participant.identity);
    renderParticipant(participant);
  });

  // When a participant disconnects
  room.on(LivekitClient.RoomEvent.ParticipantDisconnected, (participant) => {
    console.log('Participant disconnected:', participant.identity);
    removeParticipant(participant);
  });

  // When a track is subscribed
  room.on(LivekitClient.RoomEvent.TrackSubscribed, (track, publication, participant) => {
    console.log('Track subscribed:', track.kind, 'from', participant.identity);

    // Make sure the participant element exists before attaching the track
    const participantEl = document.getElementById(`participant-${participant.sid}`);
    if (!participantEl) {
      console.log('Creating element for participant:', participant.identity);
      renderParticipant(participant);
    }

    attachTrack(track, participant);
  });

  // When a track is unsubscribed
  room.on(LivekitClient.RoomEvent.TrackUnsubscribed, (track, publication, participant) => {
    console.log('Track unsubscribed:', track.kind, 'from', participant.identity);
    detachTrack(track);
  });

  room.on(LivekitClient.RoomEvent.Disconnected, (reason) => {
    setStatus('Room disconnected: ' + reason);
    joinButton.disabled = false;
    leaveButton.disabled = true;
  });
}

// Render a participant in the UI
function renderParticipant(participant) {
  if (document.getElementById(`participant-${participant.sid}`)) return;

  const participantEl = document.createElement('div');
  participantEl.id = `participant-${participant.sid}`;
  participantEl.className = 'participant';

  const nameEl = document.createElement('div');
  nameEl.className = 'participant-name';
  nameEl.textContent = participant.identity || 'Unknown';

  participantEl.appendChild(nameEl);
  videoContainer.appendChild(participantEl);

  // If participant already has tracks, attach them
  participant.trackPublications.forEach(publication => {
    if (publication.track) {
      attachTrack(publication.track, participant);
    }
  });
}

// Remove a participant from the UI
function removeParticipant(participant) {
  const participantEl = document.getElementById(`participant-${participant.sid}`);
  if (participantEl) {
    videoContainer.removeChild(participantEl);
  }
}

// Attach a track to the appropriate participant element
function attachTrack(track, participant) {
  if (track.kind !== 'video' && track.kind !== 'audio') return;

  const participantEl = document.getElementById(`participant-${participant.sid}`);
  if (!participantEl) return;

  const element = track.attach();

  if (track.kind === 'video') {
    element.style.width = '100%';
    element.style.height = '100%';
    element.style.objectFit = 'cover';
  }

  participantEl.appendChild(element);
}

// Detach a track from the DOM
function detachTrack(track) {
  track.detach();
}
