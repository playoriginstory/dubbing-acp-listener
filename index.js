import pkg from "@virtuals-protocol/acp-node";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const { default: AcpClient, AcpContractClientV2 } = pkg;

const LANGUAGES = [
  { code: "en", name: "English" }, { code: "es", name: "Spanish" },
  { code: "fr", name: "French" }, { code: "de", name: "German" },
  { code: "ja", name: "Japanese" }, { code: "zh", name: "Chinese" },
  { code: "pt", name: "Portuguese" }, { code: "hi", name: "Hindi" },
  { code: "ar", name: "Arabic" }, { code: "ru", name: "Russian" },
  { code: "ko", name: "Korean" }, { code: "it", name: "Italian" },
  { code: "nl", name: "Dutch" }, { code: "tr", name: "Turkish" },
  { code: "pl", name: "Polish" }, { code: "sv", name: "Swedish" },
  { code: "fil", name: "Filipino" }, { code: "ms", name: "Malay" },
  { code: "ro", name: "Romanian" }, { code: "uk", name: "Ukrainian" },
  { code: "el", name: "Greek" }, { code: "cs", name: "Czech" },
  { code: "da", name: "Danish" }, { code: "fi", name: "Finnish" },
  { code: "bg", name: "Bulgarian" }, { code: "hr", name: "Croatian" },
  { code: "sk", name: "Slovak" }, { code: "ta", name: "Tamil" },
  { code: "id", name: "Indonesian" },
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

const processedJobs = new Set();

const s3 = new S3Client({
  region: process.env.AWS_REGION,
});

async function uploadToS3(buffer, key, contentType) {
  await s3.send(
    new PutObjectCommand({
      Bucket: process.env.AWS_S3_BUCKET,
      Key: key,
      Body: buffer,
      ContentType: contentType,
    })
  );

  return `https://${process.env.AWS_S3_BUCKET}.s3.${process.env.AWS_REGION}.amazonaws.com/${key}`;
}

async function safeDeliver(job, status, dubbedFileUrl = "") {
  try {
    await job.deliver({
      type: "object",
      value: {
        jobId: job.id.toString(),
        status,
        dubbedFileUrl,
      },
    });
  } catch (err) {
    console.error("Delivery error:", err);
  }
}

async function processDubbing(job) {
  const { videoUrl, targetLanguage } =
    job.requirement || job.serviceRequirement || {};

  const langCode = getLanguageCode(targetLanguage);

  if (!videoUrl || !langCode) {
    await safeDeliver(job, "failed");
    return;
  }

  try {
    const dubRes = await fetch("https://duelsapp.vercel.app/api/dub", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        videoUrl,
        target_lang: langCode,
        source_lang: "auto",
      }),
    });

    const dubData = await dubRes.json();
    const dubbingId = dubData.dubbing_id;

    if (!dubbingId) {
      await safeDeliver(job, "failed");
      return;
    }

    console.log("Dubbing started:", dubbingId);

    let dubbedUrl = "";

    for (let i = 0; i < 24; i++) {
      await new Promise(r => setTimeout(r, 10000));

      const statusRes = await fetch(
        `https://duelsapp.vercel.app/api/dub-status?id=${dubbingId}`
      );
      const statusData = await statusRes.json();

      console.log(`Poll ${i + 1}:`, statusData.status);

      if (statusData.status === "dubbed") {
        const elevenRes = await fetch(
          `https://api.elevenlabs.io/v1/dubbing/${dubbingId}/audio/${langCode}`,
          {
            headers: {
              "xi-api-key": process.env.ELEVENLABS_API_KEY,
            },
          }
        );

        if (!elevenRes.ok) throw new Error("Failed to fetch dubbed file");

        const arrayBuffer = await elevenRes.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);

        const contentType =
          elevenRes.headers.get("content-type") || "audio/mpeg";

        const extension = contentType.includes("wav") ? "wav" : "mp3";

        dubbedUrl = await uploadToS3(
          buffer,
          `dubbed/${dubbingId}_${langCode}.${extension}`,
          contentType
        );

        break;
      }

      if (statusData.status === "failed") break;
    }

    if (!dubbedUrl) {
      await safeDeliver(job, "failed");
      return;
    }

    await safeDeliver(job, "completed", dubbedUrl);
    console.log("Delivered:", dubbedUrl);

  } catch (err) {
    console.error("Processing error:", err);
    await safeDeliver(job, "failed");
  }
}

async function main() {
  const acpContractClient = await AcpContractClientV2.build(
    process.env.WHITELISTED_WALLET_PRIVATE_KEY,
    parseInt(process.env.SELLER_ENTITY_ID),
    process.env.SELLER_AGENT_WALLET_ADDRESS
  );

  const acpClient = new AcpClient({
    acpContractClient,

    onNewTask: async (job, memoToSign) => {
      console.log("onNewTask fired:", job.id);

      if (memoToSign) {
        console.log("Memo to sign:", memoToSign.type, "nextPhase:", memoToSign.nextPhase);

        // Phase 0 → 1 (ACCEPT JOB)
        if (memoToSign.nextPhase === 1) {
          await job.respond(true);
          console.log("Job accepted:", job.id);
          return;
        }

        // Phase 2 → 3 (DELIVER SERVICE)
        if (memoToSign.nextPhase === 3) {
          if (processedJobs.has(job.id)) return;
          processedJobs.add(job.id);

          console.log("Processing dubbing for job:", job.id);
          await processDubbing(job);
          return;
        }
      }
    },

    onEvaluate: async (job) => {
      console.log("Evaluation phase reached:", job.id, "phase:", job.phase);

      if (job.phase !== 3) return;

      try {
        await job.evaluate(true, "Dubbing completed successfully");
        console.log("Job completed:", job.id);
      } catch (err) {
        console.error("Evaluation error:", err);
      }
    },
  });

  await acpClient.init();
  console.log("ACP seller running...");
}

main().catch(console.error);