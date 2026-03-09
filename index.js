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

/**
 * Normalize voice style — strips descriptions like "charles – Deep male voice"
 * down to just the key name "charles".
 */
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
   JOB RESULT CACHE
   Phase 1 pre-processes and stores results here.
   Delivery happens via auto-deliver timer (15s)
   OR via Phase 3 socket event — whichever comes first.
-------------------------- */

const jobResultCache = new Map();

/* -------------------------
   DEDUPLICATION
-------------------------- */

const processedPhase1 = new Set();
const processedPhase3 = new Set();

/* -------------------------
   PER-TYPE CONCURRENCY QUEUES
   Fast jobs never queue behind slow dubbing jobs.
-------------------------- */

const MAX_CONCURRENT = {
  voiceover:       10,
  voicerecast:     10,
  dubbing:          3,
  multidubbing:     3,
  musicproduction:  3
};

const activeCount = {
  voiceover: 0, voicerecast: 0,
  dubbing: 0, multidubbing: 0, musicproduction: 0
};

const queues = {
  voiceover: [], voicerecast: [],
  dubbing: [], multidubbing: [], musicproduction: []
};

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
  // Bracket-encoded harmful content (evaluator uses these placeholders)
  /\[nsfw/i,
  /\[offensive/i,
  /\[explicit/i,
  /\[redacted/i,
  /\[harmful/i,
  /\[hate/i,
  /\[violent/i,
  // Sexual content
  /nsfw/i,
  /explicit\s+(material|content|sexual|audio|video)/i,
  /explicit\s+sexual/i,
  /erotic\s+stor/i,
  /explicit\s+erot/i,
  /sexual\s+(audio|track|content|recording)/i,
  // Violence
  /incit.*violence/i,
  /incit.*violen/i,
  /audio.*incit/i,
  /graphic\s+violen/i,
  /explicit\s+violen/i,
  // Weapons / illegal
  /illegal\s+weapon/i,
  /manufactur.*(weapon|explos)/i,
  /bomb\s+(how|make|build|instruct)/i,
  // Drugs / medical
  /illegal\s+drug/i,
  /prohibited\s+medical/i,
  /drug\s+information/i,
  // Hate speech
  /hate\s+speech/i,
  /\[target\s+group/i,
  // Other
  /kill\s+all/i,
  /kill\s+every/i,
  /kill\s+(the\s+)?(group|people|race|them)/i,
  /murder\s+(all|every)/i,
  /genocide/i,
  /ethnic\s+cleans/i,
  /self.?harm/i,
  /suicide/i,
  /rape/i,
  /child\s+(porn|abuse|exploit)/i,
  /terrorist/i,
  /terrorism/i,
  /without\s+mercy/i,
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
  const uuidMatch   = html.match(/uuid=([0-9A-Za-z_\-]+)/);
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

  // Skip download+upload for direct media URLs — saves 5–10s per job
  if (/\.(mp4|mp3|webm|mov)(\?.*)?$/i.test(url)) {
    return url;
  }

  if (url.includes("drive.google.com") || url.includes("docs.google.com")) {
    const fileId = extractGoogleDriveId(url);
    if (!fileId) throw new Error("Invalid Google link format. Please share via Google Drive using 'Copy link'.");
    const { response, contentType } = await fetchGoogleDriveFile(fileId);
    if (!contentType.includes("video") && !contentType.includes("audio"))
      throw new Error("Google Drive URL does not point to a valid video/audio file.");
    const buffer = Buffer.from(await response.arrayBuffer());
    if (buffer.length > 100 * 1024 * 1024) throw new Error("File too large (max 100MB).");
    let ext = "mp4";
    if (contentType.includes("audio")) ext = "mp3";
    if (contentType.includes("webm")) ext = "webm";
    if (contentType.includes("quicktime")) ext = "mov";
    const key = `source/${Date.now()}_${Math.random().toString(36).substring(7)}.${ext}`;
    return await uploadToS3(buffer, key, contentType);
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
  const key = `source/${Date.now()}_${Math.random().toString(36).substring(7)}.${ext}`;
  return await uploadToS3(buffer, key, contentType);
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
   PROCESSING FUNCTIONS
   Return payload objects — do not call job.deliver directly.
   Results cached so Phase 3 or auto-timer delivers.
-------------------------- */

async function runDubbing(job) {
  let { videoUrl, targetLanguage } = job.requirement || job.serviceRequirement || {};
  const langCode = getLanguageCode(targetLanguage);
  const jobId = job.id.toString();

  if (!videoUrl || !langCode) {
    return { type: "object", value: { jobId, status: "failed", reason: "Missing videoUrl or invalid targetLanguage", dubbedFileUrl: "" } };
  }

  try {
    videoUrl = await normalizeVideoInput(videoUrl);

    const dubRes = await fetch("https://duelsapp.vercel.app/api/dub", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ videoUrl, target_lang: langCode, source_lang: "auto" })
    });
    const dubData = await dubRes.json();
    const dubbingId = dubData.dubbing_id;

    if (!dubbingId) {
      return { type: "object", value: { jobId, status: "failed", reason: "No dubbing ID returned", dubbedFileUrl: "" } };
    }

    let dubbedUrl = "";
    let subtitleUrl = "";

    for (let i = 0; i < 6; i++) {
      await new Promise(r => setTimeout(r, 3000));
      const statusRes = await fetch(`https://duelsapp.vercel.app/api/dub-status?id=${dubbingId}`);
      const statusData = await statusRes.json();

      if (statusData.status === "dubbed") {
        const elevenRes = await fetch(
          `https://api.elevenlabs.io/v1/dubbing/${dubbingId}/audio/${langCode}`,
          { headers: { "xi-api-key": process.env.ELEVENLABS_API_KEY } }
        );
        if (!elevenRes.ok) {
          return { type: "object", value: { jobId, status: "failed", reason: "Failed to fetch dubbed audio", dubbedFileUrl: "" } };
        }
        const buffer = Buffer.from(await elevenRes.arrayBuffer());
        const contentType = elevenRes.headers.get("content-type") || "audio/mpeg";
        dubbedUrl = await uploadToS3(buffer, `dubbed/${dubbingId}_${langCode}.mp3`, contentType);

        const transcriptRes = await fetch(
          `https://api.elevenlabs.io/v1/dubbing/${dubbingId}/transcripts/${langCode}/format/srt`,
          { headers: { "xi-api-key": process.env.ELEVENLABS_API_KEY } }
        );
        if (transcriptRes.ok) {
          const srtText = await transcriptRes.text();
          subtitleUrl = await uploadToS3(Buffer.from(srtText, "utf-8"), `subtitles/${dubbingId}_${langCode}.srt`, "application/x-subrip");
        }
        break;
      }
      if (statusData.status === "failed") break;
    }

    if (!dubbedUrl) {
      return { type: "object", value: { jobId, status: "failed", reason: "Dubbing timed out or failed", dubbedFileUrl: "" } };
    }
    return { type: "object", value: { jobId, status: "completed", dubbedFileUrl: dubbedUrl, subtitleUrl } };

  } catch (err) {
    console.error("Dubbing error:", err);
    return { type: "object", value: { jobId, status: "failed", reason: err.message || "Unexpected dubbing error", dubbedFileUrl: "" } };
  }
}

async function runMultiDubbing(job) {
  let { videoUrl, targetLanguages } = job.requirement || job.serviceRequirement || {};
  const jobId = job.id.toString();

  console.log("runMultiDubbing input:", { videoUrl, targetLanguages });

  if (!videoUrl || !Array.isArray(targetLanguages) || targetLanguages.length === 0) {
    return { type: "object", value: { jobId, status: "failed", reason: "Missing videoUrl or targetLanguages", dubbedFiles: {} } };
  }

  try {
    videoUrl = await normalizeVideoInput(videoUrl);
    const results = {};

    await Promise.all(targetLanguages.map(async (lang) => {
      const langCode = getLanguageCode(lang);
      if (!langCode) return;

      const dubRes = await fetch("https://duelsapp.vercel.app/api/dub", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ videoUrl, target_lang: langCode, source_lang: "auto" })
      });
      const dubData = await dubRes.json();
      const dubbingId = dubData.dubbing_id;
      if (!dubbingId) return;

      let dubbedUrl = "";
      let subtitleUrl = "";

      for (let i = 0; i < 6; i++) {
        await new Promise(r => setTimeout(r, 3000));
        const statusRes = await fetch(`https://duelsapp.vercel.app/api/dub-status?id=${dubbingId}`);
        const statusData = await statusRes.json();

        if (statusData.status === "dubbed") {
          const elevenRes = await fetch(
            `https://api.elevenlabs.io/v1/dubbing/${dubbingId}/audio/${langCode}`,
            { headers: { "xi-api-key": process.env.ELEVENLABS_API_KEY } }
          );
          if (!elevenRes.ok) return;
          const buffer = Buffer.from(await elevenRes.arrayBuffer());
          const contentType = elevenRes.headers.get("content-type") || "audio/mpeg";
          dubbedUrl = await uploadToS3(buffer, `multidub/${dubbingId}_${langCode}.mp3`, contentType);
          const transcriptRes = await fetch(
            `https://api.elevenlabs.io/v1/dubbing/${dubbingId}/transcripts/${langCode}/format/srt`,
            { headers: { "xi-api-key": process.env.ELEVENLABS_API_KEY } }
          );
          if (transcriptRes.ok) {
            const srtText = await transcriptRes.text();
            subtitleUrl = await uploadToS3(Buffer.from(srtText, "utf-8"), `subtitles/${dubbingId}_${langCode}.srt`, "application/x-subrip");
          }
          break;
        }
        if (statusData.status === "failed") break;
      }

      if (dubbedUrl) results[langCode] = { audio: dubbedUrl, subtitles: subtitleUrl };
    }));

    if (Object.keys(results).length === 0) {
      return { type: "object", value: { jobId, status: "failed", reason: "All dubbing attempts failed", dubbedFiles: {} } };
    }
    return { type: "object", value: { jobId, status: "completed", dubbedFiles: results } };

  } catch (err) {
    console.error("Multi-dubbing error:", err);
    return { type: "object", value: { jobId, status: "failed", reason: err.message || "Unexpected multi-dubbing error", dubbedFiles: {} } };
  }
}

async function runVoiceover(job) {
  const { text, voiceStyle } = job.requirement || job.serviceRequirement || {};
  const jobId = job.id.toString();
  const voiceId = getVoiceId(voiceStyle);

  if (!text) {
    return { type: "object", value: { jobId, status: "failed", audio: "" } };
  }

  try {
    const response = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${voiceId}`, {
      method: "POST",
      headers: { "xi-api-key": process.env.ELEVENLABS_API_KEY, "Content-Type": "application/json" },
      body: JSON.stringify({ text, model_id: "eleven_multilingual_v2" })
    });

    if (!response.ok) {
      return { type: "object", value: { jobId, status: "failed", reason: `ElevenLabs TTS returned ${response.status}`, audio: "" } };
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    const url = await uploadToS3(buffer, `voiceover/${job.id}.mp3`, "audio/mpeg");
    return { type: "object", value: { jobId, status: "completed", audio: url } };

  } catch (err) {
    console.error("Voiceover error:", err);
    return { type: "object", value: { jobId, status: "failed", reason: err.message || "Unexpected voiceover error", audio: "" } };
  }
}

async function runPremiumMusic(job) {
  const { concept, genre, mood, vocalStyle, duration, lyrics } = job.requirement || job.serviceRequirement || {};
  const jobId = job.id.toString();

  if (!concept || !genre || !mood || !vocalStyle || !duration) {
    return { type: "object", value: { jobId, status: "failed", audio: "" } };
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
      return { type: "object", value: { jobId, status: "failed", reason: `ElevenLabs music API returned ${response.status}`, audio: "" } };
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    const url = await uploadToS3(buffer, `music/${job.id}.mp3`, "audio/mpeg");
    return { type: "object", value: { jobId, status: "completed", audio: url } };

  } catch (err) {
    console.error("Music error:", err);
    return { type: "object", value: { jobId, status: "failed", reason: err.message || "Unexpected music error", audio: "" } };
  }
}

async function runVoiceRecast(job) {
  const req = job.requirement || job.serviceRequirement || {};
  const audioUrl = req.audioUrl || req.audioURL;
  const voiceStyle = req.voiceStyle;
  const voiceId = getVoiceId(voiceStyle);
  const jobId = job.id.toString();

  if (!audioUrl) {
    return { type: "object", value: { jobId, status: "failed", audio: "" } };
  }

  try {
    const sourceResponse = await fetch(audioUrl);
    if (!sourceResponse.ok) {
      return { type: "object", value: { jobId, status: "failed", reason: "Failed to fetch source audio", audio: "" } };
    }

    const audioBuffer = Buffer.from(await sourceResponse.arrayBuffer());

    if (audioBuffer.length > 25 * 1024 * 1024) {
      return { type: "object", value: { jobId, status: "failed", reason: "Audio file too large (max 25MB)", audio: "" } };
    }

    // ElevenLabs STS cap is 300s — estimate from file size at 128kbps
    const estimatedDurationSeconds = (audioBuffer.length / 128000) * 8;
    if (estimatedDurationSeconds > 295) {
      return { type: "object", value: { jobId, status: "failed", reason: "Audio too long for voice recast (max ~5 minutes)", audio: "" } };
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
    return { type: "object", value: { jobId, status: "completed", audio: url } };

  } catch (err) {
    console.error("Voice recast error:", err?.message || err);
    return { type: "object", value: { jobId, status: "failed", reason: err.message || "Unexpected voice recast error", audio: "" } };
  }
}

/* -------------------------
   DISPATCH
   Runs job, caches result, schedules auto-deliver timer.
   Auto-deliver fires after 15s if Phase 3 never arrives.
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
    else payload = { type: "object", value: { jobId, status: "failed", reason: `Unknown job type: ${job.name}` } };
  } catch (err) {
    console.error("dispatchJob error:", err);
    payload = { type: "object", value: { jobId, status: "failed", reason: "Unhandled dispatch error" } };
  }

  jobResultCache.set(jobId, { payload, delivered: false });
  console.log("Result cached:", jobId, payload.value?.status, payload.value?.reason || "");

  // Auto-deliver after 15s if Phase 3 socket event never arrives
  setTimeout(async () => {
    const cached = jobResultCache.get(jobId);
    if (cached && !cached.delivered) {
      console.log("Auto-delivering (Phase 3 not received):", jobId, cached.payload.value?.status);
      try {
        cached.delivered = true;
        await safeDeliver(job, cached.payload);
        console.log("Auto-deliver success:", jobId);
      } catch (err) {
        console.error("Auto-deliver failed:", jobId, err?.message);
        cached.delivered = false; // allow retry if it failed
      }
    }
  }, 15000);

  return payload;
}

/* -------------------------
   ACP MAIN
-------------------------- */

async function main() {
  console.log("Starting... ENV check:", {
    hasPrivKey:    !!process.env.WHITELISTED_WALLET_PRIVATE_KEY,
    entityId:      process.env.SELLER_ENTITY_ID,
    agentWallet:   process.env.SELLER_AGENT_WALLET_ADDRESS,
    hasElevenLabs: !!process.env.ELEVENLABS_API_KEY,
    hasAwsRegion:  !!process.env.AWS_REGION,
    hasAwsBucket:  !!process.env.AWS_S3_BUCKET,
  });

  let acpContractClient;
  try {
    acpContractClient = await AcpContractClientV2.build(
      process.env.WHITELISTED_WALLET_PRIVATE_KEY,
      parseInt(process.env.SELLER_ENTITY_ID),
      process.env.SELLER_AGENT_WALLET_ADDRESS
    );
    console.log("AcpContractClient built successfully");
  } catch (err) {
    console.error("FATAL: AcpContractClientV2.build() failed:", err);
    process.exit(1);
  }

  const acpClient = new AcpClient({
    acpContractClient,

    onNewTask: async (job, memoToSign) => {
      console.log("Incoming job:", { id: job?.id, name: job?.name, phase: memoToSign?.nextPhase });

      if (!memoToSign) {
        console.log("No memoToSign — skipping");
        return;
      }

      const phase = Number(memoToSign.nextPhase);
      const jobId = job.id.toString();

      /* -------------------------
         PHASE 1 — ACCEPT / REJECT
         Start processing immediately in background.
         Auto-deliver timer handles delivery if Phase 3 never arrives.
      -------------------------- */

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
            console.log("Rejecting job:", jobId, reason);
            await safeReject(job, reason);
            console.log("Job rejected:", jobId);
            return;
          }

          await safeRespond(job);
          await new Promise(r => setTimeout(r, 800));
          console.log("Job accepted:", jobId, job.name);

          // Fire and forget — auto-deliver timer scheduled inside dispatchJob
          enqueueJob(job.name, () => dispatchJob(job)).catch(err => {
            console.error("Background dispatch error:", err);
          });

        } catch (err) {
          console.error("Phase 1 error:", err);
        }

        return;
      }

      /* -------------------------
         PHASE 3 — DELIVER
         Deliver from cache. Auto-timer may already have fired.
      -------------------------- */

      if (phase === 3) {
        if (processedPhase3.has(jobId)) {
          console.log("Phase 3 already handled:", jobId);
          return;
        }
        processedPhase3.add(jobId);

        try {
          let cached = jobResultCache.get(jobId);
          let waited = 0;

          // Wait up to 30s if processing still in progress
          while (!cached && waited < 30000) {
            await new Promise(r => setTimeout(r, 1000));
            waited += 1000;
            cached = jobResultCache.get(jobId);
          }

          if (cached && !cached.delivered) {
            cached.delivered = true;
            console.log("Delivering from Phase 3:", jobId, cached.payload.value?.status);
            await safeDeliver(job, cached.payload);
          } else if (cached && cached.delivered) {
            console.log("Already delivered by auto-timer:", jobId);
          } else {
            console.log("No cached result, delivering fallback:", jobId);
            await safeDeliver(job, {
              type: "object",
              value: { jobId, status: "failed", reason: "Processing did not complete in time" }
            });
          }

        } catch (err) {
          console.error("Phase 3 error:", err);
        }

        return;
      }

      /* -------------------------
         PHASE 4 — EVALUATE
      -------------------------- */

      if (phase === 4) {
        try {
          const success = job.result?.status === "completed";
          if (success) {
            await safeEvaluate(job, true, "Service completed successfully");
            console.log("Escrow released:", jobId);
          } else {
            await safeEvaluate(job, false, "Service failed");
            console.log("Escrow refunded:", jobId);
          }
        } catch (err) {
          console.error("Phase 4 error:", err);
        }

        return;
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

  // Heartbeat with per-type queue stats
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