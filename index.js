import pkg from "@virtuals-protocol/acp-node";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const { default: AcpClient, AcpContractClientV2 } = pkg;

console.log("All env keys:", Object.keys(process.env).filter(k => k.includes("WALLET")));
console.log("Private Key:", process.env.WHITELISTED_WALLET_PRIVATE_KEY?.slice(0, 6) + "...");

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
    l => l.code.toLowerCase() === input.toLowerCase() ||
         l.name.toLowerCase() === input.toLowerCase()
  );
  return entry ? entry.code : null;
}

const processedJobs = new Set();

// Initialize S3 client
const s3 = new S3Client({ region: process.env.AWS_REGION });

// Helper to upload buffer to S3
async function uploadToS3(buffer, key, contentType) {
  await s3.send(
    new PutObjectCommand({
      Bucket: process.env.S3_BUCKET_NAME,
      Key: key,
      Body: buffer,
      ContentType: contentType,
      ACL: "public-read",
    })
  );
  return `https://${process.env.S3_BUCKET_NAME}.s3.${process.env.AWS_REGION}.amazonaws.com/${key}`;
}

async function main() {
  const acpContractClient = await AcpContractClientV2.build(
    process.env.WHITELISTED_WALLET_PRIVATE_KEY,
    parseInt(process.env.SELLER_ENTITY_ID),
    process.env.SELLER_AGENT_WALLET_ADDRESS
  );

  const acpClient = new AcpClient({
    acpContractClient,
    onNewTask: async (job) => {
      if (processedJobs.has(job.id)) {
        console.log(`Job ${job.id} already being processed, skipping`);
        return;
      }
      processedJobs.add(job.id);

      console.log("New job received:", job.id);

      const { videoUrl, targetLanguage } = job.requirement || job.serviceRequirement || {};
      const langCode = getLanguageCode(targetLanguage);

      if (!videoUrl || !langCode) {
        console.log("Missing required fields, failing job");
        await job.deliver({ jobId: job.id.toString(), status: "failed", dubbedFileUrl: "" });
        return;
      }

      try {
        // Start dubbing
        const dubRes = await fetch("https://duelsapp.vercel.app/api/dub", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ videoUrl, target_lang: langCode, source_lang: "auto" }),
        });
        const dubData = await dubRes.json();
        const dubbingId = dubData.dubbing_id;

        if (!dubbingId) {
          await job.deliver({ jobId: job.id.toString(), status: "failed", dubbedFileUrl: "" });
          return;
        }

        console.log("Dubbing started:", dubbingId);

        let dubbedUrl = "";
        // Poll for completion
        for (let i = 0; i < 24; i++) {
          await new Promise(r => setTimeout(r, 10000));
          const statusRes = await fetch(`https://duelsapp.vercel.app/api/dub-status?id=${dubbingId}`);
          const statusData = await statusRes.json();
          console.log(`Poll ${i + 1}: status = ${statusData.status}`);

          if (statusData.status === "dubbed") {
            // Fetch dubbed file from ElevenLabs
            const elevenRes = await fetch(
              `https://api.elevenlabs.io/v1/dubbing/${dubbingId}/audio/${langCode}`,
              { headers: { "xi-api-key": process.env.ELEVENLABS_API_KEY } }
            );
            if (!elevenRes.ok) throw new Error("Failed to fetch dubbed file");

            const arrayBuffer = await elevenRes.arrayBuffer();
            const buffer = Buffer.from(arrayBuffer);
            const contentType = elevenRes.headers.get("content-type") || "audio/mpeg";

            // Upload to S3
            dubbedUrl = await uploadToS3(buffer, `${dubbingId}_${langCode}.mp4`, contentType);
            break;
          }
          if (statusData.status === "failed") break;
        }

        await job.deliver({
          type: "object",
          value: {
            jobId: job.id.toString(),
            status: dubbedUrl ? "completed" : "failed",
            dubbedFileUrl: dubbedUrl,
          },
        });

        console.log("Job delivered:", dubbedUrl);
      } catch (err) {
        console.error("Error:", err);
        await job.deliver({ jobId: job.id.toString(), status: "failed", dubbedFileUrl: "" });
      }
    },
  });

  await acpClient.init();
  console.log("ACP seller listener running...");
}

main().catch(console.error);