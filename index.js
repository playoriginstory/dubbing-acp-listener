import pkg from "@virtuals-protocol/acp-node";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import FormData from "form-data";
import axios from "axios";
import { Readable } from "stream";
import fs from "fs";

if (process.env.YOUTUBE_COOKIES) {
  fs.writeFileSync("/tmp/youtube_cookies.txt", process.env.YOUTUBE_COOKIES);
}

const execFileAsync = promisify(execFile);



const { default: AcpClient, AcpContractClientV2 } = pkg;

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

const processedJobs = new Set();

async function ensurePublicVideoUrl(originalUrl) {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30000); // 30s timeout

    const response = await fetch(originalUrl, {
      signal: controller.signal
    });

    clearTimeout(timeout);

    if (!response.ok) {
      throw new Error("Failed to download source file");
    }

    const contentType =
      response.headers.get("content-type") || "application/octet-stream";

      if (!contentType.includes("video") && !contentType.includes("audio")) {
        throw new Error("URL does not point to a valid video/audio file");
      }
  
    const arrayBuffer = await response.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    // 🔒 File size limit (100MB)
    const MAX_SIZE = 100 * 1024 * 1024;
    if (buffer.length > MAX_SIZE) {
      throw new Error("File too large (max 100MB)");
    }

    // Infer extension from content type
    let extension = "mp4";
    if (contentType.includes("audio")) extension = "mp3";
    if (contentType.includes("webm")) extension = "webm";
    if (contentType.includes("quicktime")) extension = "mov";

    const key = `source/${Date.now()}_${Math.random()
      .toString(36)
      .substring(7)}.${extension}`;

    const publicUrl = await uploadToS3(buffer, key, contentType);

    return publicUrl;

  } catch (err) {
    console.error("File normalization failed:", err.message);
    throw err;
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
   VIDEO LINK NORMALIZATION
-------------------------- */

function detectSourceType(url) {
  if (url.includes("drive.google.com")) {
    return "gdrive";
  }
  if (url.includes("dropbox.com")) {
    return "dropbox";
  }
  return "direct";
}


function transformGoogleDrive(url) {
  const match = url.match(/\/d\/(.*?)\//);
  if (!match) return url;

  const fileId = match[1];
  return `https://drive.google.com/uc?export=download&id=${fileId}`;
}

function transformDropbox(url) {
  return url.replace("?dl=0", "?dl=1");
}


async function normalizeVideoInput(url) {
  // Block YouTube completely
  if (url.includes("youtube.com") || url.includes("youtu.be")) {
    throw new Error("YouTube links are not supported. Please upload a direct video file.");
  }

  const response = await fetch(url);

  if (!response.ok) {
    throw new Error("Download failed. Make sure the file is publicly accessible.");
  }

  const contentType = response.headers.get("content-type") || "";

  if (!contentType.includes("video") && !contentType.includes("audio")) {
    throw new Error("URL does not point to a valid video/audio file.");
  }

  const buffer = Buffer.from(await response.arrayBuffer());

  if (buffer.length > 100 * 1024 * 1024) {
    throw new Error("File too large (max 100MB)");
  }

  const key = `source/${Date.now()}.mp4`;

  return await uploadToS3(buffer, key, contentType);
}

function validateRequirement(jobName, req) {
  const name = normalizeJobName(jobName);
  const r = req || {};

  // IMPORTANT: your schema uses "audioURL" right now but code uses "audioUrl".
  // Support BOTH to avoid breaking.
  const audioUrl = r.audioUrl || r.audioURL;

  if (name === "dubbing") {
    if (!r.videoUrl || !isValidUrl(r.videoUrl)) return "Missing or invalid videoUrl (must be a valid http/https URL).";
    if (!r.targetLanguage) return "Missing targetLanguage.";
    if (!getLanguageCode(r.targetLanguage)) return "Unsupported targetLanguage. Please use one of the supported 29 languages.";
    return null;
  }

  if (name === "multidubbing") {
    if (!r.videoUrl || !isValidUrl(r.videoUrl)) {
      return "Missing or invalid videoUrl (must be a public http/https URL).";
    }
  
    if (!Array.isArray(r.targetLanguages)) {
      return "targetLanguages must be an array.";
    }
  
    if (r.targetLanguages.length === 0 || r.targetLanguages.length > 3) {
      return "You must provide between 1 and 3 targetLanguages.";
    }
  
    for (const lang of r.targetLanguages) {
      if (!getLanguageCode(lang)) {
        return `Unsupported language: ${lang}`;
      }
    }
  
    return null;
  }

  if (name === "voiceover") {
    if (!r.text || String(r.text).trim().length === 0) return "Missing text.";
    // optional guardrail
    if (String(r.text).length > 5000) return "Text too long (max 5000 chars).";
    if (r.voiceStyle && !VOICE_MAP[String(r.voiceStyle).toLowerCase()]) {
      return `Unsupported voiceStyle. Use one of: ${Object.keys(VOICE_MAP).join(", ")}.`;
    }
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
    if (r.lyrics && String(r.lyrics).length > 4000) return "Lyrics too long (max 4000 chars).";
    return null;
  }

  if (name === "voicerecast") {
    if (!audioUrl || !isValidUrl(audioUrl)) return "Missing or invalid audioUrl/audioURL (must be a public http/https URL).";
    if (!r.voiceStyle) return "Missing voiceStyle.";
    if (!VOICE_MAP[String(r.voiceStyle).toLowerCase()]) {
      return `Unsupported voiceStyle. Use one of: ${Object.keys(VOICE_MAP).join(", ")}.`;
    }
    return null;
  }

  return `Unknown job name "${jobName}".`;
}

/* -------------------------
   DUBBING LOGIC
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
    // 🚫 Block YouTube FIRST
    if (
      videoUrl.includes("youtube.com") ||
      videoUrl.includes("youtu.be")
    ) {
      await job.deliver({
        type: "object",
        value: {
          jobId: job.id.toString(),
          status: "failed",
          reason:
            "YouTube links are not supported. Please upload a direct video file.",
          dubbedFileUrl: ""
        }
      });
      return false;
    }

    // ✅ Only normalize AFTER blocking
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

    if (!dubbingId) throw new Error("No dubbing ID returned");

    let dubbedUrl = "";
    let subtitleUrl = "";


    for (let i = 0; i < 24; i++) {
      await new Promise(r => setTimeout(r, 10000));

      const statusRes = await fetch(
        `https://duelsapp.vercel.app/api/dub-status?id=${dubbingId}`
      );

      
      const statusData = await statusRes.json();

      if (statusData.status === "dubbed") {
        const elevenRes = await fetch(
          `https://api.elevenlabs.io/v1/dubbing/${dubbingId}/audio/${langCode}`,
          {
            headers: {
              "xi-api-key": process.env.ELEVENLABS_API_KEY
            }
          }
        );
      
        if (!elevenRes.ok) throw new Error("Failed to fetch dubbed file");
      
        const buffer = Buffer.from(await elevenRes.arrayBuffer());
        const contentType =
          elevenRes.headers.get("content-type") || "audio/mpeg";
      
        // Upload audio
        dubbedUrl = await uploadToS3(
          buffer,
          `dubbed/${dubbingId}_${langCode}.mp3`,
          contentType
        );
      
        // ✅ Fetch subtitles (SRT)
        const transcriptRes = await fetch(
          `https://api.elevenlabs.io/v1/dubbing/${dubbingId}/transcripts/${langCode}/format/srt`,
          {
            headers: {
              "xi-api-key": process.env.ELEVENLABS_API_KEY
            }
          }
        );
      
        if (!transcriptRes.ok) {
          throw new Error("Failed to fetch transcript");
        }
      
        const srtText = await transcriptRes.text();
      
        const subtitleKey = `subtitles/${dubbingId}_${langCode}.srt`;
      
        subtitleUrl = await uploadToS3(
          Buffer.from(srtText, "utf-8"),
          subtitleKey,
          "application/x-subrip"
        );
      
        break;
      }

      if (statusData.status === "failed") break;
    }

    if (!dubbedUrl) throw new Error("Dubbing failed");

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

    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "failed",
        dubbedFileUrl: ""
      }
    });
    return false;

  }
}

async function processMultiDubbing(job) {
  let { videoUrl, targetLanguages } =
    job.requirement || job.serviceRequirement || {};

  // Basic validation safety
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
    // 🚫 Block YouTube FIRST
    if (
      videoUrl.includes("youtube.com") ||
      videoUrl.includes("youtu.be")
    ) {
      await job.deliver({
        type: "object",
        value: {
          jobId: job.id.toString(),
          status: "failed",
          reason:
            "YouTube links are not supported. Please upload a direct video file.",
          dubbedFiles: {}
        }
      });
      return false;
    }

    // ✅ Only normalize AFTER blocking
    videoUrl = await normalizeVideoInput(videoUrl);

    const results = {};

    for (const lang of targetLanguages) {
      const langCode = getLanguageCode(lang);
      if (!langCode) continue;

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
      if (!dubbingId) continue;

      let dubbedUrl = "";
      let subtitleUrl = "";

      // Poll status
      for (let i = 0; i < 24; i++) {
        await new Promise(r => setTimeout(r, 10000));

        const statusRes = await fetch(
          `https://duelsapp.vercel.app/api/dub-status?id=${dubbingId}`
        );

        const statusData = await statusRes.json();

        if (statusData.status === "dubbed") {
          const elevenRes = await fetch(
            `https://api.elevenlabs.io/v1/dubbing/${dubbingId}/audio/${langCode}`,
            {
              headers: {
                "xi-api-key": process.env.ELEVENLABS_API_KEY
              }
            }
          );

          if (!elevenRes.ok) break;

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
    }

    if (Object.keys(results).length === 0) {
      await job.deliver({
        type: "object",
        value: {
          jobId: job.id.toString(),
          status: "failed",
          reason: "All dubbing attempts failed",
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
        reason: "Processing error occurred",
        dubbedFiles: {}
      }
    });

    return false;
  }
}

/* -------------------------
   VOICEOVER LOGIC (UPDATED)
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

    if (!response.ok) throw new Error("Voiceover generation failed");

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

    console.log("Voiceover delivered:", url);

  } catch (err) {
    console.error("Voiceover error:", err);

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
}

async function processPremiumMusic(job) {
  const {
    concept,
    genre,
    mood,
    vocalStyle,
    duration,
    lyrics
  } = job.requirement || job.serviceRequirement || {};

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
    // ✅ Safe duration handling (3s min, 280s max)
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

    console.log("Generating music:", {
      concept,
      genre,
      mood,
      vocalStyle,
      duration: durationMs
    });

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
          force_instrumental:
            vocalStyle.toLowerCase() === "instrumental"
        })
      }
    );

    if (!response.ok) {
      throw new Error("Music generation failed");
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

    console.log("Premium music delivered:", url);

  } catch (err) {
    console.error("Premium music error:", err);

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
}

/* -------------------------
   VOICE RECASTING LOGIC
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

    // Fetch source audio
    const sourceResponse = await fetch(audioUrl);
    if (!sourceResponse.ok) throw new Error("Failed to fetch source audio");

    const audioBuffer = Buffer.from(await sourceResponse.arrayBuffer());

    if (audioBuffer.length > 25 * 1024 * 1024) {
      throw new Error("Audio file too large");
    }

    const formData = new FormData();

    formData.append("audio", Readable.from(audioBuffer), {
      filename: "input.mp3",
      contentType: "audio/mpeg"
    });
    
    formData.append("model_id", "eleven_multilingual_sts_v2");
    
    const elevenResponse = await axios.post(
      `https://api.elevenlabs.io/v1/speech-to-speech/${voiceId}?output_format=mp3_44100_128`,
      formData,
      {
        headers: {
          "xi-api-key": process.env.ELEVENLABS_API_KEY,
          ...formData.getHeaders()
        },
        responseType: "arraybuffer"
      }
    );
    
    const resultBuffer = Buffer.from(elevenResponse.data);
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

    console.log("Voice recast delivered:", url);

  } catch (err) {
    if (err.response) {
      console.error("ELEVEN ERROR:", err.response.data?.toString());
    } else {
      console.error("Voice recast error:", err);
    }
  
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

      // --------------------
      // Phase 1: Accept / Reject
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
      // Phase 3: Process + Deliver ONLY
      // --------------------
      if (memoToSign.nextPhase === 3) {
        if (processedJobs.has(job.id)) return;
        processedJobs.add(job.id);

        try {
          if (job.name === "dubbing") {
            await processDubbing(job);
          } else if (job.name === "multidubbing") {
            await processMultiDubbing(job);
          } else if (job.name === "musicproduction") {
            await processPremiumMusic(job);
          } else if (job.name === "voiceover") {
            await processVoiceover(job);
          } else if (job.name === "voicerecast") {
            await processVoiceRecast(job);
          } else {
            console.log("Unknown job:", job.name);
          }
        } catch (err) {
          console.error("Processing error:", err);
        }

        return; // 🔥 DO NOT evaluate here
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