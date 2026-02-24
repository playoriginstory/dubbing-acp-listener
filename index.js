import pkg from "@virtuals-protocol/acp-node";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

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

/* -------------------------
   DUBBING LOGIC
-------------------------- */

async function processDubbing(job) {
  const { videoUrl, targetLanguage } =
    job.requirement || job.serviceRequirement || {};

  const langCode = getLanguageCode(targetLanguage);

  if (!videoUrl || !langCode) {
    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status: "failed",
        dubbedFileUrl: ""
      }
    });
    return;
  }

  try {
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

        dubbedUrl = await uploadToS3(
          buffer,
          `dubbed/${dubbingId}_${langCode}.mp3`,
          contentType
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
        dubbedFileUrl: dubbedUrl
      }
    });

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
    return;
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
    return;
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

      if (memoToSign.nextPhase === 1) {
        await job.respond(true);
        return;
      }

      if (memoToSign.nextPhase === 3) {
        if (processedJobs.has(job.id)) return;
        processedJobs.add(job.id);

        if (job.name === "dubbing") {
          await processDubbing(job);
          return;
        }

        if (job.name === "musicproduction") {
          await processPremiumMusic(job);
          return;
        }

        if (job.name === "voiceover") {
          await processVoiceover(job);
          return;

        }
      }
    },

    onEvaluate: async (job) => {
      if (job.phase !== 3) return;

      try {
        await job.evaluate(true, "Service completed successfully");
      } catch (err) {
        console.error("Evaluation error:", err);
      }
    }
  });

  await acpClient.init();
  console.log("ACP agent running...");
}

main().catch(console.error);