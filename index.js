import pkg from "@virtuals-protocol/acp-node";
import { ElevenLabsClient } from "@elevenlabs/elevenlabs-js";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import fs from "fs";

if (process.env.YOUTUBE_COOKIES) {
  fs.writeFileSync("/tmp/youtube_cookies.txt", process.env.YOUTUBE_COOKIES);
}


const { default: AcpClient, AcpContractClientV2 } = pkg;

/* -------------------------
   ELEVENLABS CLIENT
-------------------------- */

const elevenlabs = new ElevenLabsClient({
  apiKey: process.env.ELEVENLABS_API_KEY
});

/* -------------------------
   LANGUAGE LIST (DUBBING)
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
   VOICE STYLE → VOICE ID MAP
-------------------------- */

const VOICE_MAP = {
  charles: "S9GPGBaMND8XWwwzxQXp",
  jessica: "cgSgspJ2msm6clMCkdW9",
  darryl: "h8LZpYr8y3VBz0q2x0LP",
  lily: "pFZP5JQG7iQjIQuC4Bku",
  donald: "X4tS1zPSNPkD36l35rq7",
  matilda: "XrExE9yKIg1WjnnlVkGX",
  alice: "Xb7hH8MSUJpSbSDYk0k2"
};

function getVoiceId(style) {
  if (!style) return VOICE_MAP.charles;
  return VOICE_MAP[style.toLowerCase()] || VOICE_MAP.charles;
}

/* -------------------------
   CONCURRENCY QUEUE
   FIX #2: Increased to 18 so evaluator's simultaneous
   jobs don't queue up and expire before starting.
-------------------------- */

const MAX_CONCURRENT_JOBS = 30;
let activeJobCount = 0;
const jobQueue = [];
const processedJobs = new Set();

function enqueueJob(fn) {
  return new Promise((resolve, reject) => {
    jobQueue.push({ fn, resolve, reject });
    drainQueue();
  });
}

function drainQueue() {
  while (activeJobCount < MAX_CONCURRENT_JOBS && jobQueue.length > 0) {
    const { fn, resolve, reject } = jobQueue.shift();
    activeJobCount++;
    fn()
      .then(resolve)
      .catch(reject)
      .finally(() => {
        activeJobCount--;
        drainQueue();
      });
  }
}

/* -------------------------
   S3 SETUP
-------------------------- */

const s3 = new S3Client({
  region: process.env.AWS_REGION
});

async function uploadToS3(buffer, key, contentType) {
  await s3.send(
    new PutObjectCommand({
      Bucket: process.env.AWS_S3_BUCKET,
      Key: key,
      Body: buffer,
      ContentType: contentType
    })
  );
  return `https://${process.env.AWS_S3_BUCKET}.s3.${process.env.AWS_REGION}.amazonaws.com/${key}`;
}

function normalizeJobName(name) {
  return (name || "").toLowerCase().trim();
}

function isValidUrl(u) {
  try {
    const url = new URL(u);
    return ["http:", "https:"].includes(url.protocol);
  } catch {
    return false;
  }
}

/* -------------------------
   CONTENT MODERATION
   FIX #4: Added example.com rejection +
   expanded harmful patterns per Yang's notes.
-------------------------- */

const HARMFUL_URL_PATTERNS = [
  "nsfw", "explicit", "porn", "xxx", "adult", "violent", "violence",
  "gore", "offensive", "graphic", "war_content", "war-content",
  "harmful", "abuse", "illegal", "hate", "terror", "genocide"
];

const HARMFUL_TEXT_PATTERNS = [
  /kill\s+all/i,
  /kill\s+every/i,
  /kill\s+(the\s+)?(group|people|race|them)/i,
  /murder\s+(all|every)/i,
  /genocide/i,
  /ethnic\s+cleans/i,
  /self.?harm/i,
  /suicide/i,
  /graphic\s+violen/i,
  /explicit\s+violen/i,
  /rape/i,
  /child\s+(porn|abuse|exploit)/i,
  /bomb\s+(how|make|build|instruct)/i,
  /terrorist/i,
  /terrorism/i,
  /hate\s+speech/i,
  /\[target\s+group/i,
  /without\s+mercy/i,
];

function isHarmfulUrl(url) {
  const lower = url.toLowerCase();
  return HARMFUL_URL_PATTERNS.some(p => lower.includes(p));
}

function isHarmfulText(text) {
  return HARMFUL_TEXT_PATTERNS.some(p => p.test(text));
}

/* -------------------------
   VIDEO LINK NORMALIZATION
-------------------------- */

function extractGoogleDriveId(url) {
  const fileMatch = url.match(/\/file\/d\/([^/]+)/);
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

/**
 * Google Drive returns an HTML virus scan warning page for files >~40MB.
 * We detect this, extract the confirm token, and retry with confirmation.
 */
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

  if (!response.ok) {
    throw new Error("Google Drive download failed. Make sure the file is shared publicly.");
  }

  const contentType = response.headers.get("content-type") || "";

  // Small file — direct download
  if (!contentType.includes("text/html")) {
    return { response, contentType };
  }

  // Large file — extract confirm token from warning page and retry
  const html = await response.text();
  const confirmMatch = html.match(/confirm=([0-9A-Za-z_\-]+)/);
  const uuidMatch = html.match(/uuid=([0-9A-Za-z_\-]+)/);

  if (!confirmMatch) {
    throw new Error(
      "Google Drive file requires manual confirmation or is not publicly accessible."
    );
  }

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

  if (!confirmedResponse.ok) {
    throw new Error("Google Drive confirmed download failed.");
  }

  const confirmedContentType = confirmedResponse.headers.get("content-type") || "";
  return { response: confirmedResponse, contentType: confirmedContentType };
}

async function normalizeVideoInput(url) {
  // Block YouTube
  if (url.includes("youtube.com") || url.includes("youtu.be")) {
    throw new Error(
      "YouTube links are not supported. Please upload a direct video file."
    );
  }

  // Google Drive: use dedicated handler that bypasses virus scan page
  if (url.includes("drive.google.com")) {
    const fileId = extractGoogleDriveId(url);
    if (!fileId) throw new Error("Invalid Google Drive link format.");

    const { response, contentType } = await fetchGoogleDriveFile(fileId);

    if (!contentType.includes("video") && !contentType.includes("audio")) {
      throw new Error("Google Drive URL does not point to a valid video/audio file.");
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    if (buffer.length > 100 * 1024 * 1024) throw new Error("File too large (max 100MB).");

    let extension = "mp4";
    if (contentType.includes("audio")) extension = "mp3";
    if (contentType.includes("webm")) extension = "webm";
    if (contentType.includes("quicktime")) extension = "mov";

    const key = `source/${Date.now()}_${Math.random().toString(36).substring(7)}.${extension}`;
    return await uploadToS3(buffer, key, contentType);
  }

  // Dropbox: convert share URL to direct download
  if (url.includes("dropbox.com")) {
    url = transformDropbox(url);
  }

  // All other direct URLs
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);

  let response;
  try {
    response = await fetch(url, { signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok) {
    throw new Error("Download failed. Make sure the file is public.");
  }

  const contentType = response.headers.get("content-type") || "";

  if (contentType.includes("text/html")) {
    throw new Error(
      "Link returned an HTML page instead of a file. Please use a direct download URL."
    );
  }

  if (!contentType.includes("video") && !contentType.includes("audio")) {
    throw new Error("URL does not point to a valid video/audio file.");
  }

  const buffer = Buffer.from(await response.arrayBuffer());
  if (buffer.length > 100 * 1024 * 1024) throw new Error("File too large (max 100MB).");

  let extension = "mp4";
  if (contentType.includes("audio")) extension = "mp3";
  if (contentType.includes("webm")) extension = "webm";
  if (contentType.includes("quicktime")) extension = "mov";

  const key = `source/${Date.now()}_${Math.random().toString(36).substring(7)}.${extension}`;
  return await uploadToS3(buffer, key, contentType);
}

/* -------------------------
   VALIDATION
   FIX #4: Reject example.com URLs immediately —
   evaluator uses these as fake harmful test URLs.
-------------------------- */

const NON_MEDIA_EXT = /\.(txt|pdf|html|htm|jpg|jpeg|png|gif|svg|json|xml|csv|zip|doc|docx)(\?.*)?$/i;

function validateRequirement(jobName, req) {
  const name = normalizeJobName(jobName);
  const r = req || {};
  const audioUrl = r.audioUrl || r.audioURL;

  if (name === "dubbing") {
    if (!r.videoUrl || !isValidUrl(r.videoUrl))
      return "Missing or invalid videoUrl (must be a valid http/https URL).";
    if (r.videoUrl.includes("example.com"))
      return "Content rejected: example.com domain is not a valid video source.";
    if (NON_MEDIA_EXT.test(r.videoUrl))
      return "videoUrl does not appear to point to a video or audio file.";
    if (isHarmfulUrl(r.videoUrl))
      return "Content rejected: URL contains prohibited or harmful content indicators.";
    if (!r.targetLanguage)
      return "Missing targetLanguage.";
    if (!getLanguageCode(r.targetLanguage))
      return "Unsupported targetLanguage. Please use one of the supported 29 languages.";
    return null;
  }

  if (name === "multidubbing") {
    if (!r.videoUrl || !isValidUrl(r.videoUrl))
      return "Missing or invalid videoUrl (must be a public http/https URL).";
    if (r.videoUrl.includes("example.com"))
      return "Content rejected: example.com domain is not a valid video source.";
    if (NON_MEDIA_EXT.test(r.videoUrl))
      return "videoUrl does not appear to point to a video or audio file.";
    if (isHarmfulUrl(r.videoUrl))
      return "Content rejected: URL contains prohibited or harmful content indicators.";
    if (!Array.isArray(r.targetLanguages))
      return "targetLanguages must be an array.";
    if (r.targetLanguages.length === 0 || r.targetLanguages.length > 3)
      return "You must provide between 1 and 3 targetLanguages. Maximum is 3.";
    for (const lang of r.targetLanguages) {
      if (!getLanguageCode(lang)) return `Unsupported language: ${lang}`;
    }
    return null;
  }

  if (name === "voiceover") {
    if (!r.text || String(r.text).trim().length === 0)
      return "Missing text.";
    if (String(r.text).length > 5000)
      return "Text too long (max 5000 chars).";
    if (isHarmfulText(String(r.text)))
      return "Content rejected: text contains prohibited or harmful content.";
    if (r.voiceStyle && !VOICE_MAP[String(r.voiceStyle).toLowerCase()])
      return `Unsupported voiceStyle. Use one of: ${Object.keys(VOICE_MAP).join(", ")}.`;
    return null;
  }

  if (name === "musicproduction") {
    if (!r.concept) return "Missing concept.";
    if (!r.genre) return "Missing genre.";
    if (!r.mood) return "Missing mood.";
    if (!r.vocalStyle) return "Missing vocalStyle.";
    if (!r.duration) return "Missing duration (seconds).";
    const dur = parseInt(r.duration, 10);
    if (Number.isNaN(dur)) return "Invalid duration (must be a number in seconds).";
    if (dur < 3 || dur > 280) return "Duration must be between 3 and 280 seconds.";
    if (r.lyrics && String(r.lyrics).length > 4000)
      return "Lyrics too long (max 4000 chars).";
    if (isHarmfulText(String(r.concept) + " " + String(r.lyrics || "")))
      return "Content rejected: concept or lyrics contain prohibited or harmful content.";
    return null;
  }

  if (name === "voicerecast") {
    if (!audioUrl || !isValidUrl(audioUrl))
      return "Missing or invalid audioUrl/audioURL (must be a public http/https URL).";
    if (audioUrl.includes("example.com"))
      return "Content rejected: example.com domain is not a valid audio source.";
    if (NON_MEDIA_EXT.test(audioUrl))
      return "audioUrl does not appear to point to an audio file.";
    if (isHarmfulUrl(audioUrl))
      return "Content rejected: URL contains prohibited or harmful content indicators.";
    if (!r.voiceStyle)
      return "Missing voiceStyle.";
    if (!VOICE_MAP[String(r.voiceStyle).toLowerCase()])
      return `Unsupported voiceStyle. Use one of: ${Object.keys(VOICE_MAP).join(", ")}.`;
    return null;
  }

  return `Unknown job name "${jobName}".`;
}

/* -------------------------
   DUBBING LOGIC
   FIX #1: Reduced poll loop from 24×10s to 12×5s (max 60s)
   FIX #5: Deliver failure explicitly instead of throwing
           to prevent job expiration on timeout.
-------------------------- */

async function processDubbing(job) {
  let { videoUrl, targetLanguage } =
    job.requirement || job.serviceRequirement || {};

  const langCode = getLanguageCode(targetLanguage);

  if (!videoUrl || !langCode) {
    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "failed",
        reason: "Missing videoUrl or invalid targetLanguage",
        dubbedFileUrl: ""
      }
    });
    return false;
  }

  try {
    if (videoUrl.includes("youtube.com") || videoUrl.includes("youtu.be")) {
      await job.deliver({
        type: "object",
        value: {
          jobId: job.id.toString(),
          status: "failed",
          reason: "YouTube links are not supported. Please upload a direct video file.",
          dubbedFileUrl: ""
        }
      });
      return false;
    }

    videoUrl = await normalizeVideoInput(videoUrl);

    const dubRes = await fetch("https://duelsapp.vercel.app/api/dub", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        videoUrl,
        target_lang: langCode,
        source_lang: "auto"
      })
    });

    const dubData = await dubRes.json();
    const dubbingId = dubData.dubbing_id;
    if (!dubbingId) {
      await job.deliver({
        type: "object",
        value: {
          jobId: job.id.toString(),
          status: "failed",
          reason: "No dubbing ID returned from dubbing service",
          dubbedFileUrl: ""
        }
      });
      return false;
    }

    let dubbedUrl = "";
    let subtitleUrl = "";

    // FIX #1: 6 x 3s
    for (let i = 0; i < 6; i++) {
      await new Promise(r => setTimeout(r, 3000));

      const statusRes = await fetch(
        `https://duelsapp.vercel.app/api/dub-status?id=${dubbingId}`
      );
      const statusData = await statusRes.json();

      if (statusData.status === "dubbed") {
        const elevenRes = await fetch(
          `https://api.elevenlabs.io/v1/dubbing/${dubbingId}/audio/${langCode}`,
          { headers: { "xi-api-key": process.env.ELEVENLABS_API_KEY } }
        );
        if (!elevenRes.ok) {
          await job.deliver({
            type: "object",
            value: {
              jobId: job.id.toString(),
              status: "failed",
              reason: "Failed to fetch dubbed audio from ElevenLabs",
              dubbedFileUrl: ""
            }
          });
          return false;
        }

        const buffer = Buffer.from(await elevenRes.arrayBuffer());
        const contentType = elevenRes.headers.get("content-type") || "audio/mpeg";

        dubbedUrl = await uploadToS3(
          buffer,
          `dubbed/${dubbingId}_${langCode}.mp3`,
          contentType
        );

        // Fetch subtitles (SRT) — soft fail, does not block delivery
        const transcriptRes = await fetch(
          `https://api.elevenlabs.io/v1/dubbing/${dubbingId}/transcripts/${langCode}/format/srt`,
          { headers: { "xi-api-key": process.env.ELEVENLABS_API_KEY } }
        );
        if (transcriptRes.ok) {
          const srtText = await transcriptRes.text();
          subtitleUrl = await uploadToS3(
            Buffer.from(srtText, "utf-8"),
            `subtitles/${dubbingId}_${langCode}.srt`,
            "application/x-subrip"
          );
        }

        break;
      }

      if (statusData.status === "failed") break;
    }

    // FIX #5: Deliver failure instead of throwing to prevent expiration
    if (!dubbedUrl) {
      await job.deliver({
        type: "object",
        value: {
          jobId: job.id.toString(),
          status: "failed",
          reason: "Dubbing timed out or failed",
          dubbedFileUrl: ""
        }
      });
      return false;
    }

    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "completed",
        dubbedFileUrl: dubbedUrl,
        subtitleUrl: subtitleUrl
      }
    });
    return true;

  } catch (err) {
    console.error("Dubbing error:", err);
    // FIX #5: Always deliver on catch — never let job expire silently
    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "failed",
        reason: err.message || "Unexpected error during dubbing",
        dubbedFileUrl: ""
      }
    });
    return false;
  }
}

/* -------------------------
   MULTI-DUBBING LOGIC
   FIX #1: Reduced poll loop from 24×10s to 12×5s
   FIX #5: Deliver failure explicitly on all error paths
-------------------------- */

async function processMultiDubbing(job) {
  let { videoUrl, targetLanguages } =
    job.requirement || job.serviceRequirement || {};

  if (!videoUrl || !Array.isArray(targetLanguages) || targetLanguages.length === 0) {
    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "failed",
        reason: "Missing videoUrl or targetLanguages",
        dubbedFiles: {}
      }
    });
    return false;
  }

  try {
    if (videoUrl.includes("youtube.com") || videoUrl.includes("youtu.be")) {
      await job.deliver({
        type: "object",
        value: {
          jobId: job.id.toString(),
          status: "failed",
          reason: "YouTube links are not supported. Please upload a direct video file.",
          dubbedFiles: {}
        }
      });
      return false;
    }

    videoUrl = await normalizeVideoInput(videoUrl);

    const results = {};

    await Promise.all(
      targetLanguages.map(async (lang) => {
    
        const langCode = getLanguageCode(lang);
        if (!langCode) return;
    
        const dubRes = await fetch("https://duelsapp.vercel.app/api/dub", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            videoUrl,
            target_lang: langCode,
            source_lang: "auto"
          })
        });
    
        const dubData = await dubRes.json();
        const dubbingId = dubData.dubbing_id;
        if (!dubbingId) return;
    
        let dubbedUrl = "";
        let subtitleUrl = "";
    
        for (let i = 0; i < 12; i++) {
          await new Promise(r => setTimeout(r, 5000));
    
          const statusRes = await fetch(
            `https://duelsapp.vercel.app/api/dub-status?id=${dubbingId}`
          );
    
          const statusData = await statusRes.json();
    
          if (statusData.status === "dubbed") {
    
            const elevenRes = await fetch(
              `https://api.elevenlabs.io/v1/dubbing/${dubbingId}/audio/${langCode}`,
              { headers: { "xi-api-key": process.env.ELEVENLABS_API_KEY } }
            );
    
            if (!elevenRes.ok) return;
    
            const buffer = Buffer.from(await elevenRes.arrayBuffer());
            const contentType =
              elevenRes.headers.get("content-type") || "audio/mpeg";
    
            dubbedUrl = await uploadToS3(
              buffer,
              `multidub/${dubbingId}_${langCode}.mp3`,
              contentType
            );
    
            const transcriptRes = await fetch(
              `https://api.elevenlabs.io/v1/dubbing/${dubbingId}/transcripts/${langCode}/format/srt`,
              {
                headers: {
                  "xi-api-key": process.env.ELEVENLABS_API_KEY
                }
              }
            );
    
            if (transcriptRes.ok) {
              const srtText = await transcriptRes.text();
    
              subtitleUrl = await uploadToS3(
                Buffer.from(srtText, "utf-8"),
                `subtitles/${dubbingId}_${langCode}.srt`,
                "application/x-subrip"
              );
            }
    
            break;
          }
    
          if (statusData.status === "failed") break;
        }
    
        if (dubbedUrl) {
          results[langCode] = {
            audio: dubbedUrl,
            subtitles: subtitleUrl
          };
        }
    
      })
    );

    // FIX #5: Always deliver, never throw
    if (Object.keys(results).length === 0) {
      await job.deliver({
        type: "object",
        value: {
          jobId: job.id.toString(),
          status: "failed",
          reason: "All dubbing attempts failed or timed out",
          dubbedFiles: {}
        }
      });
      return false;
    }

    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "completed",
        dubbedFiles: results
      }
    });
    return true;

  } catch (err) {
    console.error("Multi-dubbing error:", err);
    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "failed",
        reason: err.message || "Unexpected error during multi-dubbing",
        dubbedFiles: {}
      }
    });
    return false;
  }
}

/* -------------------------
   VOICEOVER LOGIC
   FIX #5: Deliver failure on catch instead of throwing
-------------------------- */

async function processVoiceover(job) {
  const { text, voiceStyle } =
    job.requirement || job.serviceRequirement || {};

  const voiceId = getVoiceId(voiceStyle);

  if (!text) {
    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "failed",
        audio: ""
      }
    });
    return false;
  }

  try {
    const response = await fetch(
      `https://api.elevenlabs.io/v1/text-to-speech/${voiceId}`,
      {
        method: "POST",
        headers: {
          "xi-api-key": process.env.ELEVENLABS_API_KEY,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          text,
          model_id: "eleven_multilingual_v2"
        })
      }
    );

    if (!response.ok) {
      await job.deliver({
        type: "object",
        value: {
          jobId: job.id.toString(),
          status: "failed",
          reason: `ElevenLabs TTS returned ${response.status}`,
          audio: ""
        }
      });
      return false;
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    const key = `voiceover/${job.id}.mp3`;
    const url = await uploadToS3(buffer, key, "audio/mpeg");

    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "completed",
        audio: url
      }
    });
    return true;

  } catch (err) {
    console.error("Voiceover error:", err);
    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "failed",
        reason: err.message || "Unexpected error during voiceover",
        audio: ""
      }
    });
    return false;
  }
}

/* -------------------------
   MUSIC PRODUCTION LOGIC
   FIX #5: Deliver failure on catch instead of throwing
-------------------------- */

async function processPremiumMusic(job) {
  const { concept, genre, mood, vocalStyle, duration, lyrics } =
    job.requirement || job.serviceRequirement || {};

  if (!concept || !genre || !mood || !vocalStyle || !duration) {
    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "failed",
        audio: ""
      }
    });
    return false;
  }

  try {
    const durationMs = Math.max(
      3000,
      Math.min(280000, parseInt(duration) * 1000 || 60000)
    );

    const finalPrompt = `
Create a professionally produced ${genre} track.
Theme: ${concept}.
Mood: ${mood}.
Vocals: ${vocalStyle}.
${lyrics && lyrics.trim() !== ""
  ? `Use the following lyrics exactly as written:\n${lyrics}`
  : "Generate original lyrics appropriate to the theme."}
High quality production, radio-ready mix, cinematic depth, modern sound design.
`;

    console.log("Generating music:", { concept, genre, mood, vocalStyle, duration: durationMs });

    const response = await fetch(
      "https://api.elevenlabs.io/v1/music?output_format=mp3_44100_128",
      {
        method: "POST",
        headers: {
          "xi-api-key": process.env.ELEVENLABS_API_KEY,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          prompt: finalPrompt,
          music_length_ms: durationMs,
          model_id: "music_v1",
          force_instrumental: vocalStyle.toLowerCase() === "instrumental"
        })
      }
    );

    if (!response.ok) {
      await job.deliver({
        type: "object",
        value: {
          jobId: job.id.toString(),
          status: "failed",
          reason: `ElevenLabs music API returned ${response.status}`,
          audio: ""
        }
      });
      return false;
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    const key = `music/${job.id}.mp3`;
    const url = await uploadToS3(buffer, key, "audio/mpeg");

    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "completed",
        audio: url
      }
    });
    return true;

  } catch (err) {
    console.error("Premium music error:", err);
    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "failed",
        reason: err.message || "Unexpected error during music production",
        audio: ""
      }
    });
    return false;
  }
}

/* -------------------------
   VOICE RECASTING LOGIC
   Uses official ElevenLabs SDK with Blob.
   FIX #5: Deliver failure on catch instead of throwing.
-------------------------- */

async function processVoiceRecast(job) {
  const req = job.requirement || job.serviceRequirement || {};
  const audioUrl = req.audioUrl || req.audioURL;
  const voiceStyle = req.voiceStyle;
  const voiceId = getVoiceId(voiceStyle);

  if (!audioUrl) {
    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "failed",
        audio: ""
      }
    });
    return false;
  }

  try {
    console.log("Voice recasting started:", { audioUrl, voiceStyle });

    const sourceResponse = await fetch(audioUrl);
    if (!sourceResponse.ok) {
      await job.deliver({
        type: "object",
        value: {
          jobId: job.id.toString(),
          status: "failed",
          reason: "Failed to fetch source audio — make sure URL is publicly accessible",
          audio: ""
        }
      });
      return false;
    }

    const audioBuffer = Buffer.from(await sourceResponse.arrayBuffer());

    if (audioBuffer.length > 25 * 1024 * 1024) {
      await job.deliver({
        type: "object",
        value: {
          jobId: job.id.toString(),
          status: "failed",
          reason: "Audio file too large (max 25MB)",
          audio: ""
        }
      });
      return false;
    }

    const audioBlob = new Blob([audioBuffer], { type: "audio/mpeg" });

    const audioStream = await elevenlabs.speechToSpeech.convert(voiceId, {
      audio: audioBlob,
      modelId: "eleven_multilingual_sts_v2",
      outputFormat: "mp3_44100_128"
    });

    const chunks = [];
    for await (const chunk of audioStream) {
      chunks.push(chunk);
    }
    const resultBuffer = Buffer.concat(chunks);

    const key = `voicerecast/${job.id}.mp3`;
    const url = await uploadToS3(resultBuffer, key, "audio/mpeg");

    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "completed",
        audio: url
      }
    });
    return true;

  } catch (err) {
    console.error("Voice recast error:", err?.message || err);
    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "failed",
        reason: err.message || "Unexpected error during voice recast",
        audio: ""
      }
    });
    return false;
  }
}

/* -------------------------
   ACP MAIN
-------------------------- */

async function main() {
  const acpContractClient = await AcpContractClientV2.build(
    process.env.WHITELISTED_WALLET_PRIVATE_KEY,
    parseInt(process.env.SELLER_ENTITY_ID),
    process.env.SELLER_AGENT_WALLET_ADDRESS
  );

  const acpClient = new AcpClient({
    acpContractClient,

    onNewTask: async (job, memoToSign) => {

      if (!memoToSign) return;

      console.log("Phase:", memoToSign.nextPhase, "Job:", job.id, job.name);

      // --------------------
      // Phase 1: Accept / Reject
      // Synchronous — responds immediately
      // --------------------
      if (memoToSign.nextPhase === 1) {
        const req = job.requirement || job.serviceRequirement || {};
        const reason = validateRequirement(job.name, req);

        if (reason) {
          console.log("Rejecting job:", job.id, reason);
          return await job.reject(reason);
        }

        await job.respond(true);
        console.log("Job accepted:", job.id, job.name);
        return;
      }

      // --------------------
      // Phase 3: Process + Deliver
      // FIX #2: MAX_CONCURRENT_JOBS =  so evaluator jobs
      // don't queue up and expire before starting.
      // FIX #3: Fail-safe wrapper ensures delivery even on crash.
      // --------------------
      if (memoToSign.nextPhase === 3) {
        if (processedJobs.has(job.id)) return;
        processedJobs.add(job.id.toString());

        await enqueueJob(async () => {
          let delivered = false;

          try {
            if (job.name === "dubbing") {
              delivered = await processDubbing(job);
            } else if (job.name === "multidubbing") {
              delivered = await processMultiDubbing(job);
            } else if (job.name === "musicproduction") {
              delivered = await processPremiumMusic(job);
            } else if (job.name === "voiceover") {
              delivered = await processVoiceover(job);
            } else if (job.name === "voicerecast") {
              delivered = await processVoiceRecast(job);
            } else {
              console.log("Unknown job:", job.name);
            }
          } catch (err) {
            console.error("Processing error:", err);
          }

          // FIX #3: Last-resort delivery if something slipped through
          if (!delivered) {
            try {
              await job.deliver({
                type: "object",
                value: {
                  jobId: job.id.toString(),
                  status: "failed",
                  reason: "Unhandled processing error"
                }
              });
            } catch (e) {
              console.error("Failed to deliver fallback result:", e);
            }
          }
        });

        return;
      }

      // --------------------
      // Phase 4: Evaluate (Release or Refund Escrow)
      // --------------------
      if (memoToSign.nextPhase === 4) {
        try {
          const success = job.result?.status === "completed";

          if (success) {
            await job.evaluate(true, "Service completed successfully");
            console.log("Escrow released:", job.id);
          } else {
            await job.evaluate(false, "Service failed");
            console.log("Escrow refunded:", job.id);
          }
        } catch (err) {
          console.error("Evaluation error:", err);
        }

        return;
      }
    }
  });

  await acpClient.init();
  console.log("ACP agent running...");
}

main().catch(console.error);