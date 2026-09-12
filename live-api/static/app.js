// ---- Map setup ----
// Default view is a rough center of the continental US; it recenters
// automatically once the first real aircraft position arrives.
const map = L.map("map").setView([31.2, -95.1], 7);
let hasCentered = false;
 
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: "&copy; OpenStreetMap contributors",
  maxZoom: 19,
}).addTo(map);
 
const statusEl = document.getElementById("status");
 
// ---- Aircraft state ----
// One entry per ICAO, keyed by icao. Websocket messages only ever
// write into this map; the periodic loop only ever reads from it
// (to prune stale entries and refresh the status line). Marker
// position itself is updated immediately on message for responsiveness,
// since Leaflet handles that cheaply without needing a separate paint step.
const aircraft = new Map(); // icao -> { marker, lastSeen, ...fields }
 
const STALE_MS = 60_000; // remove an aircraft if we haven't heard from it in 60s
 
function upsertAircraft(update) {
  if (update.latitude == null || update.longitude == null) {
    return; // no position in this message, nothing to plot
  }
 
  const label = (update.call_sign ? update.call_sign.trim() : update.icao) +
    (update.altitude != null ? ` · ${update.altitude}ft` : "");
 
  let entry = aircraft.get(update.icao);
  if (!entry) {
    const marker = L.circleMarker([update.latitude, update.longitude], {
      radius: 5,
      color: "#4fc3f7",
      fillColor: "#4fc3f7",
      fillOpacity: 0.9,
    }).addTo(map);
    marker.bindTooltip(label, { permanent: false, direction: "top" });
 
    entry = { marker };
    aircraft.set(update.icao, entry);
  } else {
    entry.marker.setLatLng([update.latitude, update.longitude]);
    entry.marker.setTooltipContent(label);
  }
 
  entry.lastSeen = Date.now();
  entry.icao = update.icao;
  entry.callSign = update.call_sign;
  entry.altitude = update.altitude;
 
  if (!hasCentered) {
    map.setView([update.latitude, update.longitude], 8);
    hasCentered = true;
  }
}
 
function pruneStale() {
  const now = Date.now();
  for (const [icao, entry] of aircraft) {
    if (now - entry.lastSeen > STALE_MS) {
      map.removeLayer(entry.marker);
      aircraft.delete(icao);
    }
  }
}
 
// ---- Periodic maintenance loop ----
// Marker positions update immediately on message for responsiveness;
// this loop just handles cleanup and the status line on a fixed cadence.
function tick() {
  pruneStale();
  statusEl.textContent = `tracking ${aircraft.size} aircraft`;
}
setInterval(tick, 2000);
 
// ---- WebSocket connection ----
function connect() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${proto}//${location.host}/sbs`);
 
  ws.onopen = () => {
    statusEl.textContent = "connected";
  };
 
  ws.onmessage = (event) => {
    try {
      const update = JSON.parse(event.data);
      upsertAircraft(update);
    } catch (err) {
      console.error("bad message", err, event.data);
    }
  };
 
  ws.onclose = () => {
    statusEl.textContent = "disconnected, retrying...";
    setTimeout(connect, 2000);
  };
 
  ws.onerror = () => {
    ws.close();
  };
}
 
connect();