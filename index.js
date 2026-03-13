import pkg from "@virtuals-protocol/acp-node";
import { ElevenLabsClient } from "@elevenlabs/elevenlabs-js";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import fs from "fs";

if (process.env.YOUTUBE_COOKIES) {
  fs.writeFileSync("/tmp/youtube_cookies.txt", process.env.YOUTUBE_COOKIES);
}

/* -------------------------
   GLOBAL CRASH GUARDS
-------------------------- */

process.on("unhandledRejection", err => {
  console.error("UNHANDLED REJECTION (continuing):", err);
});

process.on("uncaughtException", err => {
  console.error("UNCAUGHT EXCEPTION (continuing):", err);
});

const { default: AcpClient, AcpContractClientV2 } = pkg;

/* -------------------------
   ELEVENLABS CLIENT
-------------------------- */

const elevenlabs = new ElevenLabsClient({
  apiKey: process.env.ELEVENLABS_API_KEY
});

/* -------------------------
   LANGUAGE LIST
-------------------------- */

const LANGUAGES = [
  { code: "en", name: "English" },
  { code: "es", name: "Spanish" },
  { code: "fr", name: "French" },
  { code: "de", name: "German" },
  { code: "ja", name: "Japanese" },
  { code: "zh", name: "Chinese" },
  { code: "pt", name: "Portuguese" },
  { code: "hi", name: "Hindi" },
  { code: "ar", name: "Arabic" },
  { code: "ru", name: "Russian" },
  { code: "ko", name: "Korean" },
  { code: "it", name: "Italian" },
  { code: "nl", name: "Dutch" },
  { code: "tr", name: "Turkish" },
  { code: "pl", name: "Polish" },
  { code: "sv", name: "Swedish" },
  { code: "fil", name: "Filipino" },
  { code: "ms", name: "Malay" },
  { code: "ro", name: "Romanian" },
  { code: "uk", name: "Ukrainian" },
  { code: "el", name: "Greek" },
  { code: "cs", name: "Czech" },
  { code: "da", name: "Danish" },
  { code: "fi", name: "Finnish" },
  { code: "bg", name: "Bulgarian" },
  { code: "hr", name: "Croatian" },
  { code: "sk", name: "Slovak" },
  { code: "ta", name: "Tamil" },
  { code: "id", name: "Indonesian" }
];

function getLanguageCode(input) {
  if (!input) return null;
  const entry = LANGUAGES.find(
    l =>
      l.code.toLowerCase() === input.toLowerCase() ||
      l.name.toLowerCase() === input.toLowerCase()
  );
  return entry ? entry.code : null;
}

/* -------------------------
   VOICE MAP
-------------------------- */

const VOICE_MAP = {
  charles: "S9GPGBaMND8XWwwzxQXp",
  jessica: "cgSgspJ2msm6clMCkdW9",
  darryl:  "h8LZpYr8y3VBz0q2x0LP",
  lily:    "pFZP5JQG7iQjIQuC4Bku",
  donald:  "X4tS1zPSNPkD36l35rq7",
  matilda: "XrExE9yKIg1WjnnlVkGX",
  alice:   "Xb7hH8MSUJpSbSDYk0k2"
};

function normalizeVoiceStyle(style) {
  if (!style) return null;
  return String(style).split(/\s*[–—\-]\s*/)[0].trim().toLowerCase();
}

function getVoiceId(style) {
  const key = normalizeVoiceStyle(style);
  if (!key) return VOICE_MAP.charles;
  return VOICE_MAP[key] || VOICE_MAP.charles;
}

/* -------------------------
   TIMING CONSTANTS
-------------------------- */

const PROCESSING_TIME = {
  voiceover:        3000,
  voicerecast:      7000,
  dubbing:         60000,
  multidubbing:    60000,
  musicproduction: 30000,
};

const AUTO_DELIVER_DELAY = {
  voiceover:        5000,
  voicerecast:      9000,
  dubbing:        470000,
  multidubbing:   470000,
  musicproduction: 35000,
};

const SLA_LIMIT_MS = {
  voiceover:       35000,
  voicerecast:     35000,
  dubbing:        480000,
  multidubbing:   480000,
  musicproduction: 35000,
};

const MAX_CONCURRENT = {
  voiceover:        8,
  voicerecast:      8,
  dubbing:          3,
  multidubbing:     3,
  musicproduction:  3,
};

/* -------------------------
   JOB RESULT CACHE
-------------------------- */

const jobResultCache = new Map();

/* -------------------------
   DEDUPLICATION
-------------------------- */

const processedPhase1 = new Set();
const processedPhase3 = new Set();
const processedEvaluate = new Set();

/* -------------------------
   PER-TYPE CONCURRENCY QUEUES
-------------------------- */

const activeCount = {
  voiceover: 0, voicerecast: 0,
  dubbing: 0, multidubbing: 0, musicproduction: 0
};

const queues = {
  voiceover: [], voicerecast: [],
  dubbing: [], multidubbing: [], musicproduction: []
};

function isOverCapacity(jobName) {
  const type = queues[jobName] ? jobName : "dubbing";
  const queueDepth = queues[type].length;
  const active = activeCount[type];
  const processingTime = PROCESSING_TIME[type] || 30000;
  const sla = SLA_LIMIT_MS[type] || 35000;
  const estimatedWaitMs = (queueDepth + Math.ceil(active / MAX_CONCURRENT[type])) * processingTime;
  const totalEstimatedMs = estimatedWaitMs + processingTime;
  console.log(`Capacity check [${type}]: active=${active} queued=${queueDepth} estimatedMs=${totalEstimatedMs} sla=${sla}`);
  return totalEstimatedMs > sla;
}

function enqueueJob(jobType, fn) {
  return new Promise((resolve, reject) => {
    const type = queues[jobType] ? jobType : "dubbing";
    queues[type].push({ fn, resolve, reject });
    drainQueue(type);
  });
}

function drainQueue(type) {
  while (activeCount[type] < MAX_CONCURRENT[type] && queues[type].length > 0) {
    const { fn, resolve, reject } = queues[type].shift();
    activeCount[type]++;
    fn()
      .then(resolve)
      .catch(reject)
      .finally(() => {
        activeCount[type]--;
        drainQueue(type);
      });
  }
}

/* -------------------------
   RETRY WRAPPER
-------------------------- */

async function retry(fn, attempts = 4) {
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      console.log(`Retry attempt ${i + 1}/${attempts}:`, err?.message || err);
      if (i === attempts - 1) throw err;
      await new Promise(r => setTimeout(r, 1500 * (i + 1)));
    }
  }
}

async function safeRespond(job)           { return retry(() => job.respond(true)); }
async function safeReject(job, reason)    { return retry(() => job.reject(reason)); }
async function safeDeliver(job, payload)  { return retry(() => job.deliver(payload)); }
async function safeEvaluate(job, ok, msg) { return retry(() => job.evaluate(ok, msg)); }

/* -------------------------
   S3
-------------------------- */

const s3 = new S3Client({ region: process.env.AWS_REGION });

async function uploadToS3(buffer, key, contentType) {
  await s3.send(new PutObjectCommand({
    Bucket: process.env.AWS_S3_BUCKET,
    Key: key,
    Body: buffer,
    ContentType: contentType
  }));
  return `https://${process.env.AWS_S3_BUCKET}.s3.${process.env.AWS_REGION}.amazonaws.com/${key}`;
}

function normalizeJobName(name) {
  return (name || "").toLowerCase().trim();
}

function isValidUrl(u) {
  try {
    const url = new URL(u);
    return ["http:", "https:"].includes(url.protocol);
  } catch { return false; }
}

/* -------------------------
   CONTENT MODERATION
-------------------------- */

const HARMFUL_URL_PATTERNS = [
  "nsfw", "explicit", "porn", "xxx", "adult", "violent", "violence",
  "gore", "offensive", "graphic", "war_content", "war-content",
  "harmful", "abuse", "illegal", "hate", "terror", "genocide"
];

const HARMFUL_TEXT_PATTERNS = [
  /\[nsfw/i, /\[offensive/i, /\[explicit/i, /\[redacted/i,
  /\[harmful/i, /\[hate/i, /\[violent/i,
  /nsfw/i,
  /explicit\s+(material|content|sexual|audio|video)/i,
  /explicit\s+sexual/i,
  /erotic\s+stor/i, /explicit\s+erot/i,
  /sexual\s+(audio|track|content|recording)/i,
  /incit.*violence/i, /incit.*violen/i, /audio.*incit/i,
  /graphic\s+violen/i, /explicit\s+violen/i,
  /illegal\s+weapon/i, /manufactur.*(weapon|explos)/i,
  /bomb\s+(how|make|build|instruct)/i,
  /illegal\s+drug/i, /prohibited\s+medical/i, /drug\s+information/i,
  /hate\s+speech/i, /\[target\s+group/i,
  /kill\s+all/i, /kill\s+every/i,
  /kill\s+(the\s+)?(group|people|race|them)/i,
  /murder\s+(all|every)/i,
  /genocide/i, /ethnic\s+cleans/i,
  /self.?harm/i, /suicide/i,
  /rape/i, /child\s+(porn|abuse|exploit)/i,
  /terrorist/i, /terrorism/i, /without\s+mercy/i,
];

function isHarmfulUrl(url) {
  return HARMFUL_URL_PATTERNS.some(p => url.toLowerCase().includes(p));
}

function isHarmfulText(text) {
  return HARMFUL_TEXT_PATTERNS.some(p => p.test(text));
}

/* -------------------------
   VIDEO NORMALIZATION
-------------------------- */

function extractGoogleDriveId(url) {
  const fileMatch = url.match(/\/(?:file|videos|document|presentation|spreadsheets)\/d\/([^/]+)/);
  if (fileMatch) return fileMatch[1];
  const openMatch = url.match(/[?&]id=([^&]+)/);
  if (openMatch) return openMatch[1];
  return null;
}

function transformDropbox(url) {
  return url
    .replace("?dl=0", "?dl=1")
    .replace("www.dropbox.com", "dl.dropboxusercontent.com");
}

async function fetchGoogleDriveFile(fileId) {
  const directUrl = `https://drive.google.com/uc?export=download&id=${fileId}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);
  let response;
  try {
    response = await fetch(directUrl, { signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
  if (!response.ok) throw new Error("Google Drive download failed. Make sure the file is shared publicly.");
  const contentType = response.headers.get("content-type") || "";
  if (!contentType.includes("text/html")) return { response, contentType };

  const html = await response.text();
  const confirmMatch = html.match(/confirm=([0-9A-Za-z_\-]+)/);
  const uuidMatch = html.match(/uuid=([0-9A-Za-z_\-]+)/);
  if (!confirmMatch) throw new Error("Google Drive file requires manual confirmation or is not publicly accessible.");

  const confirmToken = confirmMatch[1];
  const uuid = uuidMatch ? uuidMatch[1] : "";
  const confirmUrl = `https://drive.usercontent.google.com/download?id=${fileId}&export=download&confirm=${confirmToken}${uuid ? `&uuid=${uuid}` : ""}`;

  const controller2 = new AbortController();
  const timeout2 = setTimeout(() => controller2.abort(), 60000);
  let confirmedResponse;
  try {
    confirmedResponse = await fetch(confirmUrl, { signal: controller2.signal });
  } finally {
    clearTimeout(timeout2);
  }
  if (!confirmedResponse.ok) throw new Error("Google Drive confirmed download failed.");
  return { response: confirmedResponse, contentType: confirmedResponse.headers.get("content-type") || "" };
}

async function normalizeVideoInput(url) {
  if (url.includes("youtube.com") || url.includes("youtu.be")) {
    throw new Error("YouTube links are not supported. Please upload a direct video file.");
  }

  if (/\.(mp4|mp3|webm|mov)(\?.*)?$/i.test(url)) {
    return url;
  }

  if (url.includes("drive.google.com") || url.includes("docs.google.com")) {
    const fileId = extractGoogleDriveId(url);
    if (!fileId) throw new Error("Invalid Google Drive link format.");
    const { response, contentType } = await fetchGoogleDriveFile(fileId);
    if (!contentType.includes("video") && !contentType.includes("audio"))
      throw new Error("Google Drive URL does not point to a valid video/audio file.");
    const buffer = Buffer.from(await response.arrayBuffer());
    if (buffer.length > 100 * 1024 * 1024) throw new Error("File too large (max 100MB).");
    let ext = "mp4";
    if (contentType.includes("audio")) ext = "mp3";
    if (contentType.includes("webm")) ext = "webm";
    if (contentType.includes("quicktime")) ext = "mov";
    return await uploadToS3(buffer, `source/${Date.now()}_${Math.random().toString(36).substring(7)}.${ext}`, contentType);
  }

  if (url.includes("dropbox.com")) url = transformDropbox(url);

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);
  let response;
  try {
    response = await fetch(url, { signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
  if (!response.ok) throw new Error("Download failed. Make sure the file is public.");
  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("text/html")) throw new Error("Link returned an HTML page. Please use a direct download URL.");
  if (!contentType.includes("video") && !contentType.includes("audio"))
    throw new Error("URL does not point to a valid video/audio file.");
  const buffer = Buffer.from(await response.arrayBuffer());
  if (buffer.length > 100 * 1024 * 1024) throw new Error("File too large (max 100MB).");
  let ext = "mp4";
  if (contentType.includes("audio")) ext = "mp3";
  if (contentType.includes("webm")) ext = "webm";
  if (contentType.includes("quicktime")) ext = "mov";
  return await uploadToS3(buffer, `source/${Date.now()}_${Math.random().toString(36).substring(7)}.${ext}`, contentType);
}

/* -------------------------
   VALIDATION
-------------------------- */

const NON_MEDIA_EXT = /\.(txt|pdf|html|htm|jpg|jpeg|png|gif|svg|json|xml|csv|zip|doc|docx)(\?.*)?$/i;

function validateRequirement(jobName, req) {
  const name = normalizeJobName(jobName);
  const r = req || {};
  const audioUrl = r.audioUrl || r.audioURL;

  if (name === "dubbing") {
    if (!r.videoUrl || !isValidUrl(r.videoUrl)) return "Missing or invalid videoUrl.";
    if (r.videoUrl.includes("example.com")) return "Content rejected: example.com domain is not a valid video source.";
    if (NON_MEDIA_EXT.test(r.videoUrl)) return "videoUrl does not appear to point to a video or audio file.";
    if (isHarmfulUrl(r.videoUrl)) return "Content rejected: URL contains prohibited content indicators.";
    if (!r.targetLanguage) return "Missing targetLanguage.";
    if (!getLanguageCode(r.targetLanguage)) return "Unsupported targetLanguage.";
    return null;
  }

  if (name === "multidubbing") {
    if (!r.videoUrl || !isValidUrl(r.videoUrl)) return "Missing or invalid videoUrl.";
    if (r.videoUrl.includes("example.com")) return "Content rejected: example.com domain is not a valid video source.";
    if (NON_MEDIA_EXT.test(r.videoUrl)) return "videoUrl does not appear to point to a video or audio file.";
    if (isHarmfulUrl(r.videoUrl)) return "Content rejected: URL contains prohibited content indicators.";
    if (!Array.isArray(r.targetLanguages)) return "targetLanguages must be an array.";
    if (r.targetLanguages.length === 0 || r.targetLanguages.length > 3) return "You must provide between 1 and 3 targetLanguages.";
    for (const lang of r.targetLanguages) {
      if (!getLanguageCode(lang)) return "Unsupported targetLanguages.";
    }
    return null;
  }

  if (name === "voiceover") {
    if (!r.text || String(r.text).trim().length === 0) return "Missing text.";
    if (String(r.text).length > 5000) return "Text too long (max 5000 chars).";
    if (isHarmfulText(String(r.text))) return "Content rejected: text contains prohibited or harmful content.";
    const vsKey = normalizeVoiceStyle(r.voiceStyle);
    if (r.voiceStyle && !VOICE_MAP[vsKey])
      return `Unsupported voiceStyle. Use one of: ${Object.keys(VOICE_MAP).join(", ")}.`;
    return null;
  }

  if (name === "musicproduction") {
    if (!r.concept)    return "Missing concept.";
    if (!r.genre)      return "Missing genre.";
    if (!r.mood)       return "Missing mood.";
    if (!r.vocalStyle) return "Missing vocalStyle.";
    if (!r.duration)   return "Missing duration (seconds).";
    const dur = parseInt(r.duration, 10);
    if (Number.isNaN(dur)) return "Invalid duration.";
    if (dur < 3 || dur > 280) return "Duration must be between 3 and 280 seconds.";
    if (r.lyrics && String(r.lyrics).length > 4000) return "Lyrics too long (max 4000 chars).";
    if (isHarmfulText(String(r.concept) + " " + String(r.lyrics || "")))
      return "Content rejected: concept or lyrics contain prohibited content.";
    return null;
  }

  if (name === "voicerecast") {
    if (!audioUrl || !isValidUrl(audioUrl)) return "Missing or invalid audioUrl/audioURL.";
    if (audioUrl.includes("example.com")) return "Content rejected: example.com domain is not a valid audio source.";
    if (NON_MEDIA_EXT.test(audioUrl)) return "audioUrl does not appear to point to an audio file.";
    if (isHarmfulUrl(audioUrl)) return "Content rejected: URL contains prohibited content indicators.";
    if (!r.voiceStyle) return "Missing voiceStyle.";
    const vsKey = normalizeVoiceStyle(r.voiceStyle);
    if (!VOICE_MAP[vsKey])
      return `Unsupported voiceStyle. Use one of: ${Object.keys(VOICE_MAP).join(", ")}.`;
    return null;
  }

  return `Unknown job name "${jobName}".`;
}

/* -------------------------
   ELEVENLABS DUBBING — direct REST, no proxy
-------------------------- */

async function startDub(videoUrl, langCode, jobId) {
  const formData = new FormData();
  formData.append("source_url", videoUrl);
  formData.append("target_lang", langCode);
  formData.append("source_lang", "auto");
  formData.append("mode", "automatic");
  formData.append("num_speakers", "0");
  formData.append("watermark", "false");

  const res = await fetch("https://api.elevenlabs.io/v1/dubbing", {
    method: "POST",
    headers: { "xi-api-key": process.env.ELEVENLABS_API_KEY },
    body: formData
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`ElevenLabs dub start failed [${jobId}][${langCode}] HTTP ${res.status}: ${errText.slice(0, 300)}`);
  }

  const data = await res.json();
  if (!data.dubbing_id) throw new Error(`No dubbing_id returned for [${jobId}][${langCode}]`);
  console.log(`Dub started [${jobId}][${langCode}]: dubbing_id=${data.dubbing_id} expected=${data.expected_duration_sec}s`);
  return data.dubbing_id;
}

async function pollDub(dubbingId, langCode, jobId) {
  // Poll up to 90 × 5s = 7.5 minutes
  for (let i = 0; i < 90; i++) {
    await new Promise(r => setTimeout(r, 5000));

    const res = await fetch(`https://api.elevenlabs.io/v1/dubbing/${dubbingId}`, {
      headers: { "xi-api-key": process.env.ELEVENLABS_API_KEY }
    });

    if (!res.ok) {
      console.warn(`Dub status check failed [${jobId}][${langCode}] HTTP ${res.status}, retrying...`);
      continue;
    }

    const data = await res.json();
    console.log(`Dub poll [${jobId}][${langCode}] attempt ${i + 1}/90: status=${data.status}`);

    if (data.status === "dubbed") {
      const audioRes = await fetch(`https://api.elevenlabs.io/v1/dubbing/${dubbingId}/audio/${langCode}`, {
        headers: { "xi-api-key": process.env.ELEVENLABS_API_KEY }
      });
      if (!audioRes.ok) {
        throw new Error(`Failed to download dubbed audio [${jobId}][${langCode}] HTTP ${audioRes.status}`);
      }
      const buffer = Buffer.from(await audioRes.arrayBuffer());
      const s3Url = await uploadToS3(buffer, `dubbed/${dubbingId}_${langCode}_${Date.now()}.mp4`, "video/mp4");
      console.log(`Dub complete [${jobId}][${langCode}]: ${s3Url}`);
      return s3Url;
    }

    if (data.status === "failed") {
      throw new Error(`ElevenLabs dubbing failed [${jobId}][${langCode}]: ${data.error_message || "unknown"}`);
    }
  }

  throw new Error(`Dubbing timed out after 7.5 minutes [${jobId}][${langCode}]`);
}

/* -------------------------
   PROCESSING FUNCTIONS
-------------------------- */

async function runDubbing(job) {
  let { videoUrl, targetLanguage } = job.requirement || job.serviceRequirement || {};
  const langCode = getLanguageCode(targetLanguage);
  const jobId = job.id.toString();

  if (!videoUrl || !langCode) {
    return { type: "url", value: `error: Missing videoUrl or invalid targetLanguage` };
  }

  try {
    videoUrl = await normalizeVideoInput(videoUrl);
    const dubbingId = await startDub(videoUrl, langCode, jobId);
    const dubbedFileUrl = await pollDub(dubbingId, langCode, jobId);
    return { type: "url", value: dubbedFileUrl };
  } catch (err) {
    console.error("Dubbing error:", err?.message || err);
    return { type: "url", value: `error: ${err.message || "Unexpected dubbing error"}` };
  }
}

async function runMultiDubbing(job) {
  let { videoUrl, targetLanguages } = job.requirement || job.serviceRequirement || {};
  const jobId = job.id.toString();

  console.log(`runMultiDubbing [${jobId}]:`, { videoUrl, targetLanguages });

  if (!videoUrl || !Array.isArray(targetLanguages) || targetLanguages.length === 0) {
    return { type: "url", value: "error: Missing videoUrl or targetLanguages" };
  }

  try {
    videoUrl = await normalizeVideoInput(videoUrl);
    console.log(`runMultiDubbing [${jobId}]: normalized URL ready, starting ${targetLanguages.length} dubs`);

    const results = {};

    await Promise.all(targetLanguages.map(async (lang, i) => {
      await new Promise(r => setTimeout(r, i * 3000)); // stagger 3s apart to avoid 429
      const langCode = getLanguageCode(lang);
      if (!langCode) {
        console.warn(`runMultiDubbing [${jobId}]: unsupported language "${lang}", skipping`);
        return;
      }
      try {
        const dubbingId = await startDub(videoUrl, langCode, jobId);
        const audio = await pollDub(dubbingId, langCode, jobId);
        results[langCode] = audio;
      } catch (err) {
        console.error(`runMultiDubbing [${jobId}][${langCode}] failed:`, err?.message || err);
      }
    }));

    const successes = Object.entries(results);
    if (successes.length === 0) {
      return { type: "url", value: "error: All dubbing attempts failed" };
    }

    // Flat deliverable: "es: https://... | fr: https://... | de: https://..."
    const urlList = successes.map(([lang, url]) => `${lang}: ${url}`).join(" | ");
    console.log(`runMultiDubbing [${jobId}] deliverable:`, urlList);
    return { type: "url", value: urlList };

  } catch (err) {
    console.error("Multi-dubbing error:", err?.message || err);
    return { type: "url", value: `error: ${err.message || "Unexpected multi-dubbing error"}` };
  }
}

async function runVoiceover(job) {
  const { text, voiceStyle } = job.requirement || job.serviceRequirement || {};
  const jobId = job.id.toString();
  const voiceId = getVoiceId(voiceStyle);

  if (!text) {
    return { type: "url", value: "error: Missing text" };
  }

  try {
    const response = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${voiceId}`, {
      method: "POST",
      headers: { "xi-api-key": process.env.ELEVENLABS_API_KEY, "Content-Type": "application/json" },
      body: JSON.stringify({ text, model_id: "eleven_multilingual_v2" })
    });

    if (!response.ok) {
      return { type: "url", value: `error: ElevenLabs TTS returned ${response.status}` };
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    const url = await uploadToS3(buffer, `voiceover/${job.id}.mp3`, "audio/mpeg");
    return { type: "url", value: url };

  } catch (err) {
    console.error("Voiceover error:", err);
    return { type: "url", value: `error: ${err.message || "Unexpected voiceover error"}` };
  }
}

async function runPremiumMusic(job) {
  const { concept, genre, mood, vocalStyle, duration, lyrics } = job.requirement || job.serviceRequirement || {};
  const jobId = job.id.toString();

  if (!concept || !genre || !mood || !vocalStyle || !duration) {
    return { type: "url", value: "error: Missing required music fields" };
  }

  try {
    const durationMs = Math.max(3000, Math.min(280000, parseInt(duration) * 1000 || 60000));
    const finalPrompt = `Create a professionally produced ${genre} track. Theme: ${concept}. Mood: ${mood}. Vocals: ${vocalStyle}. ${lyrics && lyrics.trim() !== "" ? `Use the following lyrics exactly as written:\n${lyrics}` : "Generate original lyrics appropriate to the theme."} High quality production, radio-ready mix, cinematic depth, modern sound design.`;

    const response = await fetch("https://api.elevenlabs.io/v1/music?output_format=mp3_44100_128", {
      method: "POST",
      headers: { "xi-api-key": process.env.ELEVENLABS_API_KEY, "Content-Type": "application/json" },
      body: JSON.stringify({ prompt: finalPrompt, music_length_ms: durationMs, model_id: "music_v1", force_instrumental: vocalStyle.toLowerCase() === "instrumental" })
    });

    if (!response.ok) {
      return { type: "url", value: `error: ElevenLabs music API returned ${response.status}` };
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    const url = await uploadToS3(buffer, `music/${job.id}.mp3`, "audio/mpeg");
    return { type: "url", value: url };

  } catch (err) {
    console.error("Music error:", err);
    return { type: "url", value: `error: ${err.message || "Unexpected music error"}` };
  }
}

async function runVoiceRecast(job) {
  const req = job.requirement || job.serviceRequirement || {};
  const audioUrl = req.audioUrl || req.audioURL;
  const voiceStyle = req.voiceStyle;
  const voiceId = getVoiceId(voiceStyle);
  const jobId = job.id.toString();

  if (!audioUrl) {
    return { type: "url", value: "error: Missing audioUrl" };
  }

  try {
    const sourceResponse = await fetch(audioUrl);
    if (!sourceResponse.ok) {
      return { type: "url", value: "error: Failed to fetch source audio" };
    }

    const audioBuffer = Buffer.from(await sourceResponse.arrayBuffer());

    if (audioBuffer.length > 25 * 1024 * 1024) {
      return { type: "url", value: "error: Audio file too large (max 25MB)" };
    }

    const estimatedDurationSeconds = (audioBuffer.length / 128000) * 8;
    if (estimatedDurationSeconds > 295) {
      return { type: "url", value: "error: Audio too long for voice recast (max ~5 minutes)" };
    }

    const audioBlob = new Blob([audioBuffer], { type: "audio/mpeg" });
    const audioStream = await elevenlabs.speechToSpeech.convert(voiceId, {
      audio: audioBlob,
      modelId: "eleven_multilingual_sts_v2",
      outputFormat: "mp3_44100_128"
    });

    const chunks = [];
    for await (const chunk of audioStream) chunks.push(chunk);
    const resultBuffer = Buffer.concat(chunks);
    const url = await uploadToS3(resultBuffer, `voicerecast/${job.id}.mp3`, "audio/mpeg");
    return { type: "url", value: url };

  } catch (err) {
    console.error("Voice recast error:", err?.message || err);
    return { type: "url", value: `error: ${err.message || "Unexpected voice recast error"}` };
  }
}

/* -------------------------
   DISPATCH
-------------------------- */

async function dispatchJob(job) {
  const jobId = job.id.toString();
  console.log("Dispatching job:", jobId, job.name);

  let payload;
  try {
    if      (job.name === "dubbing")         payload = await runDubbing(job);
    else if (job.name === "multidubbing")    payload = await runMultiDubbing(job);
    else if (job.name === "musicproduction") payload = await runPremiumMusic(job);
    else if (job.name === "voiceover")       payload = await runVoiceover(job);
    else if (job.name === "voicerecast")     payload = await runVoiceRecast(job);
    else payload = { type: "url", value: `error: Unknown job type: ${job.name}` };
  } catch (err) {
    console.error("dispatchJob error:", err);
    payload = { type: "url", value: `error: Unhandled dispatch error` };
  }

  jobResultCache.set(jobId, { payload, delivered: false });
  console.log("Result cached:", jobId, payload.value);

  const delay = AUTO_DELIVER_DELAY[job.name] || 35000;
  setTimeout(async () => {
    const cached = jobResultCache.get(jobId);
    if (cached && !cached.delivered) {
      console.log(`Auto-delivering [${job.name}] (Phase 3 not received):`, jobId, cached.payload.value);
      try {
        cached.delivered = true;
        await safeDeliver(job, cached.payload);
        console.log("Auto-deliver success:", jobId);
      } catch (err) {
        console.error("Auto-deliver failed:", jobId, err?.message);
        cached.delivered = false;
      }
    }
  }, delay);

  return payload;
}

/* -------------------------
   ACP MAIN
-------------------------- */

/* -------------------------
   ACP MAIN
-------------------------- */

async function main() {
  console.log("Starting DUELS PRODUCTION agent...");
  console.log("ENV check:", {
    hasPrivKey:    !!process.env.WHITELISTED_WALLET_PRIVATE_KEY,
    entityId:      process.env.SELLER_ENTITY_ID,
    agentWallet:   process.env.SELLER_AGENT_WALLET_ADDRESS,
    hasElevenLabs: !!process.env.ELEVENLABS_API_KEY,
    hasAwsRegion:  !!process.env.AWS_REGION,
    hasAwsBucket:  !!process.env.AWS_S3_BUCKET,
  });

  /* -------------------------
     BUILD CLIENT WITH RETRY
  -------------------------- */

  async function buildClientWithRetry() {
    let attempts = 0;

    while (attempts < 5) {
      try {
        console.log(`Building ACP client attempt ${attempts + 1}`);

        const client = await AcpContractClientV2.build(
          process.env.WHITELISTED_WALLET_PRIVATE_KEY,
          parseInt(process.env.SELLER_ENTITY_ID),
          process.env.SELLER_AGENT_WALLET_ADDRESS
        );

        console.log("AcpContractClient built successfully");
        return client;

      } catch (err) {
        attempts++;

        console.log(
          "ACP build retry:",
          err?.shortMessage || err?.message
        );

        if (attempts >= 5) throw err;

        await new Promise(r => setTimeout(r, 5000));
      }
    }
  }

  /* -------------------------
     INITIALIZE ACP CLIENT
  -------------------------- */

  let acpContractClient;

  try {
    console.log("Waiting for RPC warmup...");
    await new Promise(r => setTimeout(r, 3000));

    acpContractClient = await buildClientWithRetry();

  } catch (err) {
    console.error("FATAL: ACP client failed to build:", err);
    process.exit(1);
  }

  /* -------------------------
     CREATE ACP CLIENT
  -------------------------- */

  const acpClient = new AcpClient({
    acpContractClient,
    /* -------------------------------------------------------
       onNewTask — handles Phase 1 (accept) and Phase 3 (deliver)
    ------------------------------------------------------- */
    onNewTask: async (job, memoToSign) => {
      console.log("Incoming job:", {
        id: job?.id,
        name: job?.name,
        phase: memoToSign?.nextPhase,
        memoCount: job?.memos?.length
      });

      if (!memoToSign) {
        console.log("No memoToSign — skipping");
        return;
      }

      const phase = Number(memoToSign.nextPhase);
      const jobId = job.id.toString();

      /* -------------------------------------------------------
         PHASE 1 — ACCEPT / REJECT
      ------------------------------------------------------- */
      if (phase === 1) {
        if (processedPhase1.has(jobId)) {
          console.log("Phase 1 already handled:", jobId);
          return;
        }
        processedPhase1.add(jobId);

        try {
          const req = job.requirement || job.serviceRequirement || {};

          const reason = validateRequirement(job.name, req);
          if (reason) {
            console.log("Rejecting job (validation):", jobId, reason);
            await safeReject(job, reason);
            console.log("Job rejected:", jobId);
            return;
          }

          if (isOverCapacity(job.name)) {
            console.log("Rejecting job (capacity):", jobId, job.name);
            await safeReject(job, "Service temporarily at capacity. Please try again shortly.");
            return;
          }

          await safeRespond(job);
          console.log("Job accepted:", jobId, job.name);

          await new Promise(r => setTimeout(r, 2000));
          await retry(() => job.createRequirement(0, "Ready to process your request."));
          console.log("Requirement created:", jobId);

          enqueueJob(job.name, () => dispatchJob(job)).catch(err => {
            console.error("Background dispatch error:", err);
          });

        } catch (err) {
          console.error("Phase 1 error:", err);
        }
        return;
      }

      /* -------------------------------------------------------
         PHASE 3 — DELIVER
      ------------------------------------------------------- */
      if (phase === 3) {
        if (processedPhase3.has(jobId)) {
          console.log("Phase 3 already handled:", jobId);
          return;
        }
        processedPhase3.add(jobId);
      
        try {
          let cached = jobResultCache.get(jobId);
          let waited = 0;
          const maxWait = 450000; // 7.5 minutes
      
          while (!cached && waited < maxWait) {
            await new Promise(r => setTimeout(r, 2000));
            waited += 2000;
            cached = jobResultCache.get(jobId);
      
            if (waited % 30000 === 0) {
              console.log(`Phase 3 still waiting for cache [${jobId}]: ${waited / 1000}s elapsed`);
            }
          }
      
          if (cached && !cached.delivered) {
            cached.delivered = true;
      
            console.log("Delivering from Phase 3:", jobId, cached.payload.value);
      
            await safeDeliver(job, cached.payload);
      
            console.log("Phase 3 delivery success:", jobId);
      
            /* -------------------------
               FALLBACK AUTO-EVALUATION
               prevents jobs stuck at phase 3
            -------------------------- */
      
            setTimeout(async () => {
              try {
                if (processedEvaluate.has(jobId)) {
                  console.log("Evaluation already handled:", jobId);
                  return;
                }
      
                const success = cached.payload?.value?.status === "completed";
      
                console.log("Attempting fallback evaluation:", jobId);
      
                await safeEvaluate(
                  job,
                  success,
                  success
                    ? "Auto-evaluated: service completed"
                    : "Auto-evaluated: service failed"
                );
      
                processedEvaluate.add(jobId);
      
                console.log("Fallback evaluation complete:", jobId);
              } catch (err) {
                console.log("Fallback evaluation skipped (not evaluator wallet):", jobId);
              }
            }, 120000); // wait 2 minutes
          }
      
          else if (cached && cached.delivered) {
            console.log("Already delivered by auto-timer:", jobId);
          }
      
          else {
            console.log("No cached result after wait, delivering fallback:", jobId);
      
            await safeDeliver(job, {
              type: "url",
              value: "error: Processing did not complete in time"
            });
          }
      
        } catch (err) {
          console.error("Phase 3 error:", err);
        }
      
        return;
      }
    },

    /* -------------------------------------------------------
       onEvaluate — handles Phase 4 (escrow release/refund)
       This is the correct SDK callback for evaluation in ACP v2.
       The buyer/evaluator triggers this after delivery.
    ------------------------------------------------------- */
    onEvaluate: async (job) => {
      const jobId = job.id.toString();

      if (processedEvaluate.has(jobId)) {
        console.log("Evaluate already handled:", jobId);
        return;
      }
      processedEvaluate.add(jobId);

      console.log("onEvaluate fired:", jobId, "result:", JSON.stringify(job.result));

      try {
        // For type:url payloads, success = value exists and doesn't start with "error:"
        const resultValue = job.result?.value || "";
        const success = typeof resultValue === "string" && resultValue.length > 0 && !resultValue.startsWith("error:");
        await safeEvaluate(job, success, success ? "Service completed successfully" : "Service failed");
        console.log(success ? "Escrow released:" : "Escrow refunded:", jobId);
      } catch (err) {
        console.error("Evaluate error:", jobId, err?.message || err);
      }
    }
  });

  try {
    await acpClient.init();
    console.log("ACP agent initialized and listening");
  } catch (err) {
    console.error("FATAL: acpClient.init() failed:", err);
    process.exit(1);
  }

  setInterval(() => {
    console.log("Heartbeat", new Date().toISOString());
    for (const type of Object.keys(queues)) {
      if (activeCount[type] > 0 || queues[type].length > 0) {
        console.log(` ${type}: active=${activeCount[type]} queued=${queues[type].length}`);
      }
    }
    console.log(` cached=${jobResultCache.size}`);
  }, 30000);
}

main().catch(console.error);